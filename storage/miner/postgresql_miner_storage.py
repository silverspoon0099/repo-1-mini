from collections import defaultdict
import os
import threading
import contextlib
import datetime as dt
import bittensor as bt
from typing import Dict, List, Iterator, Optional, Tuple
import json
import hashlib
import time
from pathlib import Path
from filelock import FileLock
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row, namedtuple_row, scalar_row
from psycopg import Connection
from sqlalchemy import create_engine, text
import pandas as pd
import re
from dotenv import load_dotenv
import traceback

load_dotenv(dotenv_path=".env", override=True)

from common import constants, utils
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from storage.miner.miner_storage import MinerStorage


# Shared flag file in data-universe root
PAUSE_FLAG_PATH = Path(__file__).resolve().parents[3] / "pause_bucket_summary.json"
PAUSE_FLAG_LOCK = FileLock(str(PAUSE_FLAG_PATH) + ".lock")

def set_pause_flag(hotkey: str, paused: bool):
    with PAUSE_FLAG_LOCK:
        data = {}
        if PAUSE_FLAG_PATH.exists():
            data = json.loads(PAUSE_FLAG_PATH.read_text())

        if "status" not in data and all(isinstance(v, bool) for v in data.values()):
            # Legacy format — upgrade
            data = {"status": data}

        data["status"] = data.get("status", {})
        data["status"][hotkey] = paused

        PAUSE_FLAG_PATH.write_text(json.dumps(data))

def get_bad_validators() -> set[str]:
    """Returns a set of hotkeys marked as 'bad' in the shared PAUSE_FLAG_PATH JSON."""
    with PAUSE_FLAG_LOCK:
        if not PAUSE_FLAG_PATH.exists():
            return set()
        try:
            data = json.loads(PAUSE_FLAG_PATH.read_text())
            bad = data.get("bad_validators", [])
            if isinstance(bad, list):
                return {str(x) for x in bad}
        except Exception:
            pass
    return set()

def _hour_label_from_timebucket(tb: TimeBucket) -> str:
    date_range = TimeBucket.to_date_range(tb)
    start_dt = date_range.start
    dt_aligned = start_dt.replace(minute=0, second=0, microsecond=0)
    return dt_aligned.strftime("%Y%m%d%H")


class PostgreSQLMinerStorage(MinerStorage):
    def __init__(
        self, 
        use_compress=False, 
        database=None, 
        max_database_size_gb_hint=250, 
        wallet=None,
    ):
        max_size = max(int(os.getenv('PG_MAX_CONNECTION', '0')), 20)
        self.pg_pool = ConnectionPool(
            conninfo=f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@"
                     f"{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', 5432)}/{os.getenv('PG_DB')}",
            min_size=1,
            max_size=max_size,
            kwargs={"row_factory": dict_row},
            open=True  # Ensures pool is initialized immediately
        )
        
        self.engine = create_engine(
            f"postgresql+psycopg://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@"
            f"{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', 5432)}/{os.getenv('PG_DB')}"
        )
        self.use_compress = use_compress
        self.database_max_content_size_bytes = utils.gb_to_bytes(max_database_size_gb_hint)
        self.max_age_in_hours = constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24
        self.wallet = wallet
        if self.wallet:
            self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.cached_index_refresh_lock = threading.Lock()
        self.cached_index_lock = threading.Lock()
        self.cached_index_4 = None
        self.cached_index_updated = dt.datetime.min
        self.last_summary_ts = dt.datetime.utcnow()

    def _create_connection(self) -> Connection:
        """Gets a connection from the psycopg3 pool."""
        return self.pg_pool.connection()

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""
        return None

    def refresh_compressed_index(self, date_time: dt.timedelta):
        """Refreshes the compressed MinerIndex."""
        return None

    def _set_last_summary_timestamp(self, ts: dt.datetime) -> None:
        """Persist last_summary_ts in DB (UTC), single-row table (id=1)."""
        # Ensure timezone-aware UTC
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        else:
            ts = ts.astimezone(dt.timezone.utc)

        with self._create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO "BucketSummaryCheckpoint" (id, last_summary_ts)
                    VALUES (1, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET last_summary_ts = EXCLUDED.last_summary_ts;
                    """,
                    (ts,),
                )
            conn.commit()

    def _get_last_summary_timestamp(self) -> dt.datetime:
        """Load last_summary_ts from DB; if missing, return now(UTC)."""
        with self._create_connection() as conn:
            with conn.cursor(row_factory=scalar_row) as cur:
                cur.execute('SELECT last_summary_ts FROM "BucketSummaryCheckpoint" WHERE id = 1;')
                ts = cur.fetchone()

        if ts is None:
            # First run: seed with now (UTC)
            return dt.datetime.now(dt.timezone.utc)

        # psycopg already returns timestamptz as aware datetime
        return ts.astimezone(dt.timezone.utc)
        
    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId, using batch handling."""
        
        if data_entity_bucket_id.source == DataSource.X.value and data_entity_bucket_id.label is None:
            return self.list_data_entities_in_data_entity_big_bucket(data_entity_bucket_id)

        label = "NULL" if data_entity_bucket_id.label is None else data_entity_bucket_id.label.value
        
        batch_size = 5000
        data_entities = []
        running_size = 0
        date_range = TimeBucket.to_date_range(data_entity_bucket_id.time_bucket)
        
        last_uri = None
        has_more = True
        
        last_summary_ts = self._get_last_summary_timestamp()
        
        if self.wallet:
            set_pause_flag(self.miner_hotkey, True)

        try:
            with self._create_connection() as conn:
                with conn.cursor() as temp_cursor:  # unnamed cursor
                    temp_cursor.execute("SET enable_partition_pruning = on;")
                    temp_cursor.execute("SET enable_partitionwise_join = on;")
                    
                while has_more:
                    query = """
                        SELECT
                            m.uri,
                            c.datetime,
                            m.source,
                            c.content,
                            m."contentSizeBytes",
                            m.label,
                            m.uri_hash
                        FROM (
                            SELECT uri_hash, uri, source, label, "contentSizeBytes"
                            FROM "DataEntityMetaDaily"
                            WHERE
                                "timeBucketId" = %s
                                AND source = %s
                                AND label = %s
                                AND created_at <= %s
                        ) AS m
                        JOIN (
                            SELECT uri_hash, datetime, content
                            FROM "DataEntityContentDaily"
                            WHERE 
                                source = %s
                                AND label = %s
                                AND datetime >= %s
                                AND datetime < %s
                        ) AS c ON m.uri_hash = c.uri_hash
                    """
                    
                    if last_uri:
                        query += " AND m.uri_hash > %s"

                    query += " ORDER BY m.uri_hash LIMIT %s"

                    params = [
                        data_entity_bucket_id.time_bucket.id,
                        data_entity_bucket_id.source,
                        label,
                        last_summary_ts,
                        data_entity_bucket_id.source,
                        label,
                        date_range.start,
                        date_range.end,
                    ]

                    if last_uri:
                        params.append(last_uri)

                    params.append(batch_size)

                    with conn.cursor() as cursor:
                        cursor.execute(query, params)
                        rows = cursor.fetchall()

                    if not rows:
                        has_more = False
                        break

                    for row in rows:
                        if running_size >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
                            bt.logging.info(
                                f"📦 Bucket {data_entity_bucket_id} reached byte limit ({running_size} bytes), stopping fetch early."
                            )
                            return data_entities

                        data_entity = DataEntity(
                            uri=row["uri"],
                            datetime=row["datetime"],
                            source=DataSource(row["source"]),
                            content=bytes(row["content"]),
                            content_size_bytes=row["contentSizeBytes"],
                            label=DataLabel(value=row["label"]) if row["label"] != "NULL" else None,
                        )

                        data_entities.append(data_entity)
                        running_size += row["contentSizeBytes"]

                    last_uri = row["uri_hash"]

            bt.logging.trace(
                f"Returning {len(data_entities)} data entities for bucket {data_entity_bucket_id} with {running_size} bytes"
            )
            return data_entities

        except Exception as e:
            bt.logging.error(f"❌ Failed to list data entities for bucket {data_entity_bucket_id}: {e}")
            return []
        finally:
            if self.wallet:
                set_pause_flag(self.miner_hotkey, False)

    def list_data_entities_in_data_entity_big_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Efficiently lists data from the specific partition of 'DataEntityBigBucket' using a server-side cursor."""
        
        label = "NULL" if data_entity_bucket_id.label is None else data_entity_bucket_id.label.value
        hour_label = _hour_label_from_timebucket(data_entity_bucket_id.time_bucket)
        partition_name = f'"DataEntityBigBucket_{hour_label}"'

        batch_size = 5000
        data_entities = []
        running_size = 0
        last_summary_ts = self._get_last_summary_timestamp()

        if self.wallet:
            set_pause_flag(self.miner_hotkey, True)

        try:
            with self._create_connection() as conn:
                with conn.cursor() as setup_cursor:
                    setup_cursor.execute("SET TRANSACTION READ ONLY;")
                
                with conn.cursor(name="bigbucket_cursor", row_factory=namedtuple_row, binary=True) as cursor:
                    cursor.execute(f"""
                        SELECT uri, datetime, content, "contentSizeBytes"
                        FROM {partition_name}
                        WHERE "timeBucketId" = %s
                        AND created_at <= %s;
                    """, (
                        data_entity_bucket_id.time_bucket.id,
                        last_summary_ts,
                    ))

                    while True:
                        rows = cursor.fetchmany(batch_size)  # psycopg3: arraysize is fetchmany default if omitted
                        if not rows:
                            break

                        for row in rows:
                            if running_size >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
                                bt.logging.info(
                                    f"📦 Big bucket {data_entity_bucket_id} reached byte limit "
                                    f"({running_size} bytes), stopping early."
                                )
                                # Named cursor will be closed by context manager
                                return data_entities

                            # row.content is a memoryview (binary=True); avoid bytes() copy
                            data_entities.append(
                                DataEntity(
                                    uri=row.uri,
                                    datetime=row.datetime,
                                    source=DataSource(DataSource.X.value),  # table is X-only
                                    content=row.content,                     # zero-copy
                                    content_size_bytes=row.contentSizeBytes,
                                    label=None,
                                )
                            )
                            running_size += row.contentSizeBytes

            bt.logging.trace(
                f"Returning {len(data_entities)} big bucket data entities for {data_entity_bucket_id} with {running_size} bytes"
            )
            return data_entities

        except Exception as e:
            bt.logging.error(f"❌ Failed to list big bucket data entities for {data_entity_bucket_id}: {e}")
            return []

        finally:
            if self.wallet:
                set_pause_flag(self.miner_hotkey, False)

    def list_contents_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[bytes]]:
        """Lists contents for each requested DataEntityBucketId."""

        if (
            len(data_entity_bucket_ids) == 0
            or len(data_entity_bucket_ids) > constants.BULK_BUCKETS_COUNT_LIMIT
        ):
            return defaultdict(list)

        meta_clauses = []
        content_clauses = []
        params = {}

        for i, bucket_id in enumerate(data_entity_bucket_ids):
            label = "NULL" if bucket_id.label is None else bucket_id.label.value
            source = bucket_id.source
            tb_id = bucket_id.time_bucket.id
            date_range = TimeBucket.to_date_range(bucket_id.time_bucket)

            meta_clauses.append(
                f'("timeBucketId" = :tb_{i} AND source = :src_{i} AND label = :lbl_{i})'
            )
            content_clauses.append(
                f'(source = :src_{i} AND label = :lbl_{i} AND datetime BETWEEN :start_{i} AND :end_{i})'
            )

            params.update({
                f'tb_{i}': tb_id,
                f'src_{i}': source,
                f'lbl_{i}': label,
                f'start_{i}': date_range.start,
                f'end_{i}': date_range.end,
            })

        query = f"""
            WITH
            filtered_meta AS (
                SELECT uri_hash, "timeBucketId", source, label, "contentSizeBytes"
                FROM "DataEntityMetaDaily"
                WHERE {' OR '.join(meta_clauses)}
            ),
            filtered_content AS (
                SELECT uri_hash, datetime, content
                FROM "DataEntityContentDaily"
                WHERE {' OR '.join(content_clauses)}
            )
            SELECT fm."timeBucketId", fm.source, fm.label, fc.content, fm."contentSizeBytes"
            FROM filtered_meta fm
            JOIN filtered_content fc USING (uri_hash)
            LIMIT {constants.BULK_CONTENTS_COUNT_LIMIT};
        """

        with self._create_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET enable_partition_pruning = on;")
                cursor.execute("SET enable_partitionwise_join = on;")

                cursor.execute(text(query), params)

                buckets_ids_to_contents = defaultdict(list)
                running_size = 0

                for row in cursor:
                    if running_size >= constants.BULK_CONTENTS_SIZE_LIMIT_BYTES:
                        break

                    bucket_id = DataEntityBucketId(
                        time_bucket=TimeBucket(id=row["timeBucketId"]),
                        source=DataSource(row["source"]),
                        label=DataLabel(value=row["label"]) if row["label"] != "NULL" else None,
                    )
                    buckets_ids_to_contents[bucket_id].append(bytes(row["content"]))
                    running_size += row["contentSizeBytes"]

                return buckets_ids_to_contents

    def get_compressed_index(
        self,
        bucket_count_limit: int = constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
        hotkey: Optional[str] = None,
    ) -> CompressedMinerIndex:
        """
        Gets the compressed MinerIndex from the shared MinerCompressedIndex table.
        If the caller hotkey is in bad_validators (PAUSE_FLAG_PATH), restrict sources to {REDDIT, X}.

        New behavior: 
        - uses COMPRESS_RANGE_FACTOR env var (default 1)
        - first limits to bucket_count_limit * COMPRESS_RANGE_FACTOR ordered by score
        - then from that pool, depending on MAIN_MINER_ORDER_BY_SCORE, either keep top-scored
            or random order, finally limit to bucket_count_limit
        """
        # Parse order_by_score flag
        order_by_score = str(os.getenv("MAIN_MINER_ORDER_BY_SCORE", "false")).strip().lower() in ("1", "true", "yes", "on")
        if order_by_score:
            bt.logging.debug("MAIN_MINER_ORDER_BY_SCORE is ON: final ordering by score DESC.")
        else:
            bt.logging.debug("MAIN_MINER_ORDER_BY_SCORE is OFF: final ordering by random().")

        # Parse compress_range_factor, ensure it's an integer >= 1
        try:
            crf_raw = os.getenv("COMPRESS_RANGE_FACTOR", "")
            compress_range_factor = float(crf_raw)
            if compress_range_factor < 1:
                bt.logging.warning(
                    f"COMPRESS_RANGE_FACTOR env var is less than 1 ({compress_range_factor}); using 1 instead."
                )
                compress_range_factor = 1
        except (ValueError, TypeError):
            bt.logging.warning(
                f"COMPRESS_RANGE_FACTOR env var is not set or invalid ('{crf_raw}'); using 1."
            )
            compress_range_factor = 1

        # Compute the first phase limit
        initial_limit = bucket_count_limit * compress_range_factor

        # Read once outside lock
        bad_set = get_bad_validators()
        restrict_sources = hotkey is not None and hotkey in bad_set
        if restrict_sources:
            bt.logging.info(f"Restricting compressed index for bad validator hotkey={hotkey} to Reddit+X.")

        with self.cached_index_lock:
            try:
                with self._create_connection() as conn:
                    with conn.cursor() as cursor:
                        # Optimize
                        cursor.execute("SET work_mem = '1GB';")

                        # Build WHERE clause
                        where_sql = ""
                        params: list = []

                        if restrict_sources:
                            reddit_value = DataSource.REDDIT.value
                            x_value = DataSource.X.value
                            where_sql = "WHERE source IN (%s, %s)"
                            params.extend([reddit_value, x_value])

                        # First phase: select initial_limit rows ordered by score DESC
                        # We always use score DESC in first phase to get the top scoring range
                        first_phase_sql = f"""
                            SELECT source, label, "timeBucketId", "bucketSize", score
                            FROM "MinerCompressedIndex"
                            {where_sql}
                            ORDER BY score DESC
                            LIMIT %s
                        """
                        params_first = params.copy()
                        params_first.append(initial_limit)

                        cursor.execute(first_phase_sql, tuple(params_first))

                        # Fetch into Python list
                        rows = list(cursor.fetchall())

                        # If fewer rows than bucket_count_limit, no need for further filtering
                        if len(rows) <= bucket_count_limit:
                            selected = rows
                        else:
                            if order_by_score:
                                # Keep top bucket_count_limit by score (rows already ordered by score DESC)
                                selected = rows[:bucket_count_limit]
                            else:
                                selected = rows.copy()
                                random.shuffle(selected)
                                selected = selected[:bucket_count_limit]

                        # Now build the output from selected rows
                        buckets_by_source_by_label = defaultdict(dict)

                        for row in selected:
                            source = row["source"]
                            label = row["label"] if row["label"] != "NULL" else None
                            time_bucket_id = row["timeBucketId"]
                            bucket_size = min(constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES, row["bucketSize"])
                            source_enum = DataSource(source)

                            bucket = buckets_by_source_by_label[source_enum].get(
                                label, CompressedEntityBucket(label=label)
                            )
                            bucket.sizes_bytes.append(bucket_size)
                            bucket.time_bucket_ids.append(time_bucket_id)
                            buckets_by_source_by_label[source_enum][label] = bucket

                        compressed_index = CompressedMinerIndex(
                            sources={
                                source: list(label_map.values())
                                for source, label_map in buckets_by_source_by_label.items()
                            }
                        )

                        self.cached_index_4 = compressed_index
                        return compressed_index

            except Exception as e:
                bt.logging.error(f"❌ Failed to fetch compressed index from DB: {e}")
                return self.cached_index_4
    
    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all the DataEntities that this MinerStorage is currently serving."""
        try:
            with self._create_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SET enable_partition_pruning = on;")
                    cursor.execute("SET enable_partitionwise_aggregate = on;")

                    oldest_time_bucket_id = TimeBucket.from_datetime(
                        dt.datetime.now() - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
                    ).id

                    # Inject the literal value for optimal partition pruning
                    cursor.execute(f"""
                        SELECT SUM("contentSizeBytes") AS "bucketSize", "timeBucketId", source, label 
                        FROM "DataEntityMetaDaily"
                        WHERE "timeBucketId" >= {oldest_time_bucket_id}
                        GROUP BY "timeBucketId", source, label
                        ORDER BY "bucketSize" DESC
                        LIMIT {constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX};
                    """)

                    data_entity_buckets = []
                    for row in cursor:
                        bucket_size, time_bucket_id, source, label = row

                        size = min(bucket_size, constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)

                        data_entity_bucket_id = DataEntityBucketId(
                            time_bucket=TimeBucket(id=time_bucket_id),
                            source=DataSource(source),
                            label=DataLabel(value=label) if label != "NULL" else None,
                        )

                        data_entity_buckets.append(
                            DataEntityBucket(id=data_entity_bucket_id, size_bytes=size)
                        )

                    return data_entity_buckets
        except Exception as e:
            bt.logging.error(f"❌ Failed to list data entity buckets: {e}")
            return []

    def close_pool(self):
        """Closes all connections in the PostgreSQL connection pool."""
        if self.pg_pool and not self.pg_pool.closed:  # assuming self.pool is your ConnectionPool instance
            self.pg_pool.close()
            bt.logging.info("✅ PostgreSQL connection pool closed.")


def test_list_data_entities_in_data_entity_bucket(bucket_id = None):
    storage = PostgreSQLMinerStorage()
    
    # Example bucket datetime
    bucket2_datetime = dt.datetime(2025, 9, 24, 12, 43, 12, tzinfo=dt.timezone.utc)  # update if needed

    if not bucket_id:
        bucket_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=DataSource.YOUTUBE.value,
            label=DataLabel(value="#ytc_c_abp-news"),
        )

    # Test: batch-handled version
    print("🧱 Starting list_data_entities_in_data_entity_bucket test")
    start_time = time.time()
    results = storage.list_data_entities_in_data_entity_bucket(bucket_id)    
    duration = time.time() - start_time
    print(f"✅ list_data_entities_in_data_entity_bucket returned {len(results)} rows in {duration:.2f} seconds")
    return results[:3]
    

if __name__ == "__main__":
    bt.logging.set_trace()
    test_list_data_entities_in_data_entity_bucket()