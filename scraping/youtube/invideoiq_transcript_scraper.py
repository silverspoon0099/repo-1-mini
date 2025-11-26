import asyncio
import traceback
import re
import bittensor as bt
from typing import List, Dict, Any, Optional, Tuple
import datetime as dt
import httpx
import os
from pathlib import Path
from filelock import FileLock
import json
import isodate
from dotenv import load_dotenv
load_dotenv(override=True)

from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.youtube.model import YouTubeContent
from scraping.youtube import utils as youtube_utils
from scraping.apify import ActorRunner, RunConfig, ActorRunError
from utils.custom_logging import setup_bt_logging
from apify_client import ApifyClient

class YouTubeChannelTranscriptScraper(Scraper):
    """
    Final YouTube scraper using:
    1. invideoiq/video-transcript-scraper for transcripts and language support
    2. Minimal YouTube API for upload dates and video discovery
    3. Channel-based labeling (no language in labels)
    4. Respects miner's original language choices during validation
    """

    # Apify actor ID for the video transcript scraper
    ACTOR_ID = "invideoiq/video-transcript-scraper"

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 1

    # Timeout for Apify actor runs
    APIFY_TIMEOUT_SECS = 180

    # Default language for transcripts (ISO 639-1 format)
    DEFAULT_LANGUAGE = "en"
    
    STATISTICS_DELAY_HOURS = 48

    def __init__(self, runner: ActorRunner = None, yt_api_key=None, storage=None):
        """Initialize the YouTube Channel Transcript Scraper."""
        self.runner = runner or ActorRunner()

        # Minimal YouTube API setup for upload dates and video discovery
        self.youtube_api_key = yt_api_key or os.getenv("YOUTUBE_API_KEY")
        if not self.youtube_api_key:
            bt.logging.warning("YOUTUBE_API_KEY not found - upload date validation and channel discovery will be limited")

        # HTTP client for YouTube API calls
        self._http_timeout = httpx.Timeout(15.0)
        
        self.storage = storage
        self.scraped_video_ids: set[str] = set()
        script_dir = Path(__file__).resolve().parent
        self._cache_file = str(script_dir / "youtube_channel_cache.json")
        self._cache_lock = FileLock(self._cache_file + ".lock")
        self._channel_cache = self._load_channel_cache()
        
        bt.logging.info("YouTube Channel Transcript Scraper initialized")

    def _load_channel_cache(self) -> Dict[str, Dict[str, str]]:
        if not os.path.exists(self._cache_file):
            return {}
        with self._cache_lock:
            try:
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}

    def _save_channel_cache(self):
        with self._cache_lock:
            with open(self._cache_file, "w", encoding="utf-8") as f:
                json.dump(self._channel_cache, f, indent=2)

    async def scrape(
        self,
        scrape_config: Optional[ScrapeConfig] = None,
        youtube_url: Optional[str] = None,
        channel_url: Optional[str] = None,
        language: str = "en",
        max_videos: int = 3,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[DataEntity]:
        """
        Scrapes YouTube transcripts with channel-based approach or single video.
        Language is determined automatically by the actor for each video.
        """
        # Handle single video scraping (multi-actor scraper compatibility)
        if youtube_url:
            return await self._scrape_single_video(youtube_url, language)
        
        # Handle channel scraping with ScrapeConfig
        if scrape_config:
            bt.logging.info(f"Starting YouTube channel scrape with config: {scrape_config}")
            
            # Parse channel identifiers from labels
            channel_identifiers = self._parse_channel_configs_from_labels(scrape_config.labels)

            if not channel_identifiers:
                bt.logging.warning("No channel identifiers found in labels")
                return [], False

            return await self._scrape_channels_hybrid(channel_identifiers, scrape_config), True
        
        bt.logging.error("Must provide either youtube_url or scrape_config")
        return []

    def _parse_channel_configs_from_labels(self, labels: List[DataLabel]) -> List[str]:
        """
        Parse channel configurations from labels.

        Supported format: #ytc_c_{channel_identifier}
        """
        channel_identifiers = []

        if not labels:
            bt.logging.warning("No labels provided")
            return channel_identifiers

        bt.logging.trace(f"Parsing {len(labels)} labels: {[label.value for label in labels]}")

        for label in labels:
            bt.logging.trace(f"Processing label: {label.value}")
            channel_identifier = YouTubeContent.parse_channel_label(label.value)
            if channel_identifier:
                channel_identifiers.append(channel_identifier)
                bt.logging.trace(
                    f"Successfully parsed channel identifier: '{channel_identifier}' from label: '{label.value}'")
            else:
                bt.logging.warning(f"Failed to parse channel identifier from label: '{label.value}'")

        bt.logging.trace(f"Found {len(channel_identifiers)} valid channel identifiers: {channel_identifiers}")
        return channel_identifiers
    
    async def _scrape_channels_hybrid(self, channel_identifiers: List[str], scrape_config: ScrapeConfig, ondemand: bool = True) -> List[
        DataEntity]:
        """
        Scrape videos from channels using hybrid approach.
        Lets the actor choose the best available language for each video.
        """
        results = []
        max_entities = scrape_config.entity_limit or 10

        for channel_identifier in channel_identifiers:
            try:
                if not ondemand and not self.scraped_video_ids and self.storage:
                    self.scraped_video_ids = await asyncio.to_thread(
                        self.storage.get_scraped_videos_by_channel_id,
                        channel_identifier
                    )

                bt.logging.info(f"Processing channel: {channel_identifier}")

                # Get recent videos from the channel
                channel_info, video_data = await self._get_channel_videos(channel_identifier, scrape_config.date_range, max_entities)

                if not video_data:
                    bt.logging.warning(f"No videos found for channel {channel_identifier}")
                    continue

                bt.logging.info(f"Found {len(video_data)} videos for channel {channel_identifier}")

                # Process each video
                for i, video_info in enumerate(video_data, 1):
                    try:
                        video_id: str = video_info["video_id"]
                        upload_date: dt.datetime = video_info["published_at"]
                        title: str = video_info.get("title", "")
                        description: Optional[str] = video_info.get("description")
                        view_count: int = int(video_info.get("view_count", 0))
                        like_count: int = int(video_info.get("like_count", 0))
                        
                        bt.logging.trace(f"Processing video {i}/{len(video_data)}: {video_id}")

                        if view_count < 100:
                            bt.logging.trace(f"Video {video_id} has only {view_count} views, skipping (minimum 100 required)")
                            continue
                        
                        # Try to get transcript in any available language (let actor decide)
                        transcript_data = await self._get_transcript_from_actor_any_language(video_id)
                        if not transcript_data:
                            bt.logging.trace(f"No transcript available for video {video_id}")
                            continue

                        # # Get upload date from API
                        # upload_date = await self._get_upload_date_from_api(video_id)
                        # if not upload_date:
                        #     bt.logging.warning(f"No upload date for video {video_id}, using current time")
                        #     upload_date = dt.datetime.now(dt.timezone.utc)

                        # Get view count from Apify actor response
                        # view_count = transcript_data.get('view_count', 0)
                        
                        # # Check date range
                        # if not self._is_within_date_range(upload_date, scrape_config.date_range):
                        #     bt.logging.trace(f"Video {video_id} outside date range, skipping")
                        #     continue

                        # Use the language that the actor actually returned
                        selected_language = transcript_data.get('selected_language', self.DEFAULT_LANGUAGE)
                        transcript = transcript_data.get("transcript", [])
                        duration_seconds = int(transcript_data.get('duration', '0'))
                        if not youtube_utils.custom_validate_transcript_timing(transcript, duration_seconds):
                            continue
                        detected_language = youtube_utils.detect_transcript_language(transcript)
                        
                        actual_language = detected_language or selected_language
                        bt.logging.trace(f"Video {video_id} transcript obtained in language: {actual_language}")
                        if title != transcript_data.get('title'):
                            bt.logging.trace(f"Title mismatched: {title[:100]} != {transcript_data.get('title')[:100]}")
                            transcript_data["title"] = title
                    
                        # Get video statistics from API (view_count, like_count)
                        # statistics = await self._get_video_statistics_from_api(video_id)
                        # if statistics:
                        #     transcript_data['view_count'] = statistics.get('view_count', transcript_data.get('view_count'))
                        #     transcript_data['like_count'] = statistics.get('like_count')

                        transcript_data['view_count'] = view_count or transcript_data.get('view_count')
                        transcript_data['like_count'] = like_count
                        
                        transcript_data['description'] = description

                        # # Get subscriber count from API
                        # channel_id = transcript_data.get('channel_id')
                        # if channel_id:
                        #     subscriber_count = await self._get_channel_subscriber_count(channel_id)
                        #     transcript_data['subscriber_count'] = subscriber_count
                        transcript_data['subscriber_count'] = int(channel_info.get("subscriber_count", 0))

                        # Create content
                        content = self._create_youtube_content(
                            video_id=video_id,
                            language=actual_language,
                            upload_date=upload_date,
                            transcript_data=transcript_data,
                            channel_identifier=channel_identifier
                        )

                        # Convert to DataEntity
                        entity = YouTubeContent.to_data_entity(content)
                        results.append(entity)

                        bt.logging.trace(f"Successfully scraped video {video_id} from channel {channel_identifier} in language {actual_language}")

                        if len(results) >= max_entities:
                            bt.logging.info(f"{channel_identifier} Reached maximum entities limit ({max_entities})")
                            break

                    except Exception as e:
                        bt.logging.error(f"{channel_identifier} Error scraping video {video_id}: {str(e)}")
                        bt.logging.debug(traceback.format_exc())
                        continue

                if len(results) >= max_entities:
                    break

            except Exception as e:
                bt.logging.error(f"Error scraping channel {channel_identifier}: {str(e)}")
                bt.logging.debug(traceback.format_exc())
                continue

        bt.logging.success(f"{channel_identifier} Scraped {len(results)} YouTube videos from channels")
        return results

    async def _get_channel_videos(
        self,
        channel_identifier: str,
        date_range: DateRange,
        max_videos: int = 10
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Get channel info + recent videos (with stats) using YouTube API.
        Caches channel_id, uploads_playlist_id, subscriber_count for 24h.

        Returns:
        (
            channel_info: {
                "channel_id": str,
                "uploads_playlist_id": str,
                "subscriber_count": int,
                "cached_at": str (UTC ISO)
            },
            video_data: List[{
                "video_id": str,
                "published_at": datetime,
                "title": str,
                "view_count": int,
                "like_count": int
            }]
        )
        """
        if not self.youtube_api_key:
            bt.logging.warning("No YouTube API key - cannot discover channel videos")
            return {}, []

        try:
            bt.logging.trace(f"Discovering videos for channel: {channel_identifier}")

            cache_entry: Optional[Dict[str, Any]] = self._channel_cache.get(channel_identifier)
            channel_info: Optional[Dict[str, Any]] = None

            if cache_entry:
                # Check staleness (24h)
                cached_at_raw = cache_entry.get("cached_at")
                is_stale = True
                if cached_at_raw:
                    try:
                        cached_at = dt.datetime.fromisoformat(cached_at_raw)
                        if cached_at.tzinfo is None:
                            cached_at = cached_at.replace(tzinfo=dt.timezone.utc)
                        age = dt.datetime.now(dt.timezone.utc) - cached_at
                        is_stale = age >= dt.timedelta(hours=24)
                    except Exception:
                        is_stale = True

                if not is_stale:
                    channel_info = {
                        "channel_id": cache_entry["channel_id"],
                        "uploads_playlist_id": cache_entry["uploads_playlist_id"],
                        "subscriber_count": int(cache_entry.get("subscriber_count", 0)),
                        "cached_at": cache_entry.get("cached_at"),
                    }
                    bt.logging.info(f"Loaded cached channel info for '{channel_identifier}'")
                else:
                    bt.logging.info(f"Cache stale for '{channel_identifier}', refreshing channel info")
            
            if channel_info is None:
                # Resolve channel ID (when first time / stale / not cached)
                if not cache_entry:
                    channel_id = await self._resolve_channel_id(channel_identifier)
                    if not channel_id:
                        bt.logging.warning(f"Could not resolve channel ID for: {channel_identifier}")
                        return {}, []
                    bt.logging.info(f"Resolved channel '{channel_identifier}' to ID: {channel_id}")
                else:
                    channel_id = cache_entry["channel_id"]

                # Fetch uploads playlist + subscriber count
                fresh_info = await self._get_channel_info(channel_id)
                if not fresh_info:
                    return {}, []

                channel_info = fresh_info

                # Save to cache
                self._channel_cache[channel_identifier] = {
                    "channel_id": channel_info["channel_id"],
                    "uploads_playlist_id": channel_info["uploads_playlist_id"],
                    "subscriber_count": channel_info["subscriber_count"],
                    "cached_at": channel_info["cached_at"],
                }
                self._save_channel_cache()
                bt.logging.info(f"Cached channel data for '{channel_identifier}'")

            # Fetch playlist videos
            uploads_playlist_id = channel_info["uploads_playlist_id"]
            video_data = await self._get_playlist_videos(uploads_playlist_id, date_range, max_videos)

            return channel_info, video_data

        except Exception as e:
            bt.logging.error(f"Error getting videos for channel {channel_identifier}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return {}, []
    
    async def _resolve_channel_id(self, channel_identifier: str) -> Optional[str]:
        """Resolve channel identifier to actual channel ID."""
        try:
            # If it looks like a channel ID already (starts with UC), return it
            if channel_identifier.startswith('UC') and len(channel_identifier) == 24:
                bt.logging.info(f"Channel identifier '{channel_identifier}' appears to be a channel ID")
                return channel_identifier

            # Try to search for the channel by name/handle
            bt.logging.trace(f"Searching for channel: {channel_identifier}")
            url = "https://www.googleapis.com/youtube/v3/search"
            params = {
                "q": channel_identifier,
                "type": "channel",
                "part": "snippet",
                "maxResults": 1,
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items"):
                    channel_id = data["items"][0]["snippet"]["channelId"]
                    bt.logging.trace(f"Found channel ID: {channel_id}")
                    return channel_id
                else:
                    bt.logging.warning(f"No search results for channel: {channel_identifier}")

        except Exception as e:
            bt.logging.error(f"Error resolving channel ID for {channel_identifier}: {str(e)}")

        return None

    async def _get_channel_info(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch uploads playlist ID and subscriber count for a channel.
        Returns:
            {
            "channel_id": str,
            "uploads_playlist_id": str,
            "subscriber_count": int,
            "cached_at": str (UTC ISO 8601)
            }
        or None on failure.
        """
        try:
            url = "https://www.googleapis.com/youtube/v3/channels"
            params = {
                "id": channel_id,
                "part": "contentDetails,statistics",
                "key": self.youtube_api_key,
            }
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            items = data.get("items", [])
            if not items:
                bt.logging.warning(f"No channel data found for ID: {channel_id}")
                return None

            item = items[0]
            uploads_playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
            stats = item.get("statistics", {})
            # subscriberCount can be hidden -> not present
            subscriber_count = int(stats.get("subscriberCount", 0))

            info = {
                "channel_id": channel_id,
                "uploads_playlist_id": uploads_playlist_id,
                "subscriber_count": subscriber_count,
                "cached_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
            return info

        except Exception as e:
            bt.logging.error(f"Error fetching channel info for {channel_id}: {e}")
            bt.logging.debug(traceback.format_exc())
            return None
    
    async def _get_playlist_videos(
        self,
        playlist_id: str,
        date_range: DateRange,
        max_videos: int
    ) -> List[Dict[str, Any]]:
        """Get videos from a playlist with snippet+statistics."""
        video_data: List[Dict[str, Any]] = []

        try:
            bt.logging.info(f"Getting videos from playlist: {playlist_id}")
            url = "https://www.googleapis.com/youtube/v3/playlistItems"
            params = {
                "playlistId": playlist_id,
                "part": "contentDetails",
                "maxResults": min(max_videos, 50),
                "key": self.youtube_api_key,
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                raw_items = data.get("items", [])
                if not raw_items:
                    bt.logging.warning(f"{playlist_id} No items found in playlist response")
                    return []

                # Extract unique, unscripted video IDs from playlist items
                video_ids: List[str] = []
                for item in raw_items:
                    try:
                        video_id = item["contentDetails"]["videoId"]
                        if video_id not in self.scraped_video_ids:
                            video_ids.append(video_id)
                    except Exception as e:
                        bt.logging.error(f"{playlist_id} Error extracting videoId: {str(e)}")

                if not video_ids:
                    bt.logging.warning(f"{playlist_id} No valid video IDs to process")
                    return []

                # Batch request for snippet+statistics
                stats_url = "https://www.googleapis.com/youtube/v3/videos"
                stats_params = {
                    "id": ",".join(video_ids),
                    "part": "snippet,statistics",
                    "key": self.youtube_api_key,
                }

                stats_response = await client.get(stats_url, params=stats_params)
                stats_response.raise_for_status()
                stats_data = stats_response.json()

                for item in stats_data.get("items", []):
                    try:
                        video_id = item["id"]

                        published_at_str = item["snippet"]["publishedAt"]
                        published_at = dt.datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                        if published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=dt.timezone.utc)

                        if not self._is_within_date_range(published_at, date_range):
                            bt.logging.trace(f"Video {video_id} outside date range, skipping")
                            continue

                        title = item.get("snippet", {}).get("title", "")
                        description = item.get("snippet", {}).get("description")
                        statistics = item.get("statistics", {}) or {}
                        view_count = int(statistics.get("viewCount", 0))
                        like_count = int(statistics.get("likeCount", 0))

                        video_data.append({
                            "video_id": video_id,
                            "published_at": published_at,
                            "title": title,
                            "description": description,
                            "view_count": view_count,
                            "like_count": like_count,
                        })

                    except Exception as e:
                        bt.logging.error(f"{playlist_id} Error processing video data: {str(e)}")

                # Enforce max_videos if needed (after date filtering)
                if len(video_data) > max_videos:
                    video_data = sorted(video_data, key=lambda x: x["published_at"], reverse=True)[:max_videos]

                bt.logging.success(f"Retrieved {len(video_data)} videos with view/like counts from playlist")

        except Exception as e:
            bt.logging.error(f"{playlist_id} Error getting playlist videos: {str(e)}")

        return video_data

    async def _get_transcript_from_actor_any_language(self, video_id: str, channel_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get transcript data using the Apify actor - let it choose the best available language."""
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            run_input = {
                "video_url": video_url,
                "best_effort": True,
                "get_yt_original_metadata": True
            }

            run_config = RunConfig(
                actor_id=self.ACTOR_ID,
                debug_info=f"Transcript for {video_id} (any language)",
                max_data_entities=1,
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            bt.logging.trace(f"Invideoiq Getting transcript for video {video_id} (any available language)")

            dataset = await self.runner.run(run_config, run_input)

            if not dataset or len(dataset) == 0:
                bt.logging.trace(f"Invideoiq do not returned transcript data for video {video_id}")
                return None

            result = dataset[0]
            selected_language = result.get('selected_language', 'unknown')
            available_languages = result.get('available_languages', [])
            transcript = result.get('transcript', [])
            if not transcript:
                bt.logging.trace(f"Invideoiq No transcript for video {video_id}")
                return None
            
            bt.logging.trace(f"Invideoiq Got transcript for {video_id} in language: {selected_language}")
            if available_languages:
                bt.logging.trace(f"Available languages: {', '.join(available_languages)}")

            # 🔧 Normalize transcript: start/end as .3 strings, duration = end - start
            result = youtube_utils.normalize_actor_transcript(
                result=result, 
                channel_identifier=channel_identifier,
                cheat=False
            )

            return result

        except Exception as e:
            bt.logging.trace(f"Invideoiq Error getting transcript for video {video_id}: {str(e)}")
            return None

    async def _get_transcript_from_actor(self, video_id: str, language: str, channel_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get transcript data using the Apify actor with specific language."""
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            apify_api_key = os.getenv("APIFY_API_TOKEN")
            client = ApifyClient(apify_api_key)
            run_input = {
                "video_url": video_url,
                "language": language,
                "best_effort": True,
                "get_yt_original_metadata": True
            }

            run_config = RunConfig(
                actor_id=self.ACTOR_ID,
                debug_info=f"Transcript for {video_id} in {language}",
                max_data_entities=1,
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            dataset = await self.runner.run(run_config, run_input)

            if not dataset or len(dataset) == 0 or not dataset[0].get("transcript", ""):
                bt.logging.debug(
                    f"Invideoiq Getting transcript for video {video_id} language {language} failed. Now trying with residential proxy")
                return None
            
                max_retry = 2
                run_input["use_residential_proxy_for_yt"] = True

                for try_cnt in range(max_retry):
                    await asyncio.sleep(5)
                    run = client.actor("invideoiq/video-transcript-scraper").call(run_input=run_input)
                    bt.logging.info("💾 Check your data here: https://console.apify.com/storage/key-value-stores/" + run[
                        "defaultKeyValueStoreId"])
                    output_data = client.key_value_store(run["defaultKeyValueStoreId"]).get_record('OUTPUT')['value']
                    if not output_data or not output_data.get("transcript", ""):
                        if try_cnt == max_retry - 1:
                            return None
                        continue
                    result = youtube_utils.normalize_actor_transcript(output_data)
                    return result
            result = dataset[0]
            result = youtube_utils.normalize_actor_transcript(
                result=result, 
                channel_identifier=channel_identifier,
                cheat=False
            )
            return result

        except Exception as e:
            bt.logging.error(f"Invideoiq Error getting transcript for video {video_id} in {language}: {str(e)}")
            return None

    async def _get_upload_date_from_api(self, video_id: str) -> Optional[dt.datetime]:
        """Get upload date from YouTube API."""
        if not self.youtube_api_key:
            return None

        try:
            url = "https://www.googleapis.com/youtube/v3/videos"
            params = {
                "id": video_id,
                "part": "snippet",
                "fields": "items(snippet(publishedAt))",
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items") and len(data["items"]) > 0:
                    item = data["items"][0]
                    published_at = item["snippet"]["publishedAt"]
                    upload_date = dt.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    return upload_date

        except Exception as e:
            bt.logging.warning(f"Failed to get upload date for {video_id}: {str(e)}")

        return None

    async def _get_video_statistics_from_api(self, video_id: str) -> dict:
        """Get video statistics (view_count, like_count) from YouTube API."""
        if not self.youtube_api_key:
            return {}

        try:
            url = "https://www.googleapis.com/youtube/v3/videos"
            params = {
                "id": video_id,
                "part": "statistics",
                "fields": "items(statistics(viewCount,likeCount))",
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items") and len(data["items"]) > 0:
                    item = data["items"][0]
                    statistics = item.get("statistics", {})
                    return {
                        "view_count": int(statistics.get("viewCount", 0)),
                        "like_count": int(statistics.get("likeCount", 0))
                    }

        except Exception as e:
            bt.logging.warning(f"Failed to get statistics for {video_id}: {str(e)}")

        return {}

    async def _get_channel_subscriber_count(self, channel_id: str) -> Optional[int]:
        """Get subscriber count for a channel from YouTube API."""
        if not self.youtube_api_key:
            return None

        try:
            url = "https://www.googleapis.com/youtube/v3/channels"
            params = {
                "id": channel_id,
                "part": "statistics",
                "fields": "items(statistics(subscriberCount))",
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items") and len(data["items"]) > 0:
                    item = data["items"][0]
                    statistics = item.get("statistics", {})
                    return int(statistics.get("subscriberCount", 0))

        except Exception as e:
            bt.logging.warning(f"Failed to get subscriber count for channel {channel_id}: {str(e)}")

        return None

    def _create_youtube_content(self, video_id: str, language: str, upload_date: dt.datetime,
                                transcript_data: Dict[str, Any], channel_identifier: Optional[str] = None) -> YouTubeContent:
        """Create YouTubeContent object from scraped data."""

        # Extract channel info
        channel_name = transcript_data.get('channel', '')

        # Use the provided channel_identifier as channel_id (normalized)

        return YouTubeContent(
            video_id=video_id,
            title=transcript_data.get('title', ''),
            channel_name=channel_name,
            upload_date=upload_date,
            transcript=transcript_data.get('transcript', []),
            url=f"https://www.youtube.com/watch?v={video_id}",
            duration_seconds=int(transcript_data.get('duration', '0')),
            language=language,
            description=transcript_data.get('description'),
            thumbnails=youtube_utils.generate_thumbnails(video_id),
            view_count=transcript_data.get('view_count'),
            like_count=transcript_data.get('like_count'),
            subscriber_count=(transcript_data.get('subscriber_count') or None)
        )

    async def _get_single_video_metadata(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Return (title, published_at, title) for a single video_id.
        - published_at is tz-aware (UTC) datetime parsed from snippet.publishedAt
        - returns None on error or not found
        """
        if not self.youtube_api_key:
            bt.logging.warning("YOUTUBE_API_KEY missing - cannot fetch video metadata")
            return None

        try:
            url = "https://www.googleapis.com/youtube/v3/videos"
            params = {
                "id": video_id,
                "part": "snippet,statistics",
                "key": self.youtube_api_key,
            }
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                r = await client.get(url, params=params)
                r.raise_for_status()
                data = r.json()

            items = data.get("items", [])
            if not items:
                return None

            snippet = items[0].get("snippet", {}) or {}
            statistics = items[0].get("statistics", {}) or {}
            title = snippet.get("title", "") or ""
            description = snippet.get("description")
            view_count = int(statistics.get("viewCount", 0))
            like_count = int(statistics.get("likeCount", 0))
            published_at_str = (snippet.get("publishedAt") or "").replace("Z", "+00:00")

            published_at = None
            if published_at_str:
                try:
                    published_at = dt.datetime.fromisoformat(published_at_str)
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    published_at = dt.datetime.now(dt.timezone.utc)
            else:
                published_at = dt.datetime.now(dt.timezone.utc)

            # as requested: (title, published_at, title)
            return {
                "title": title,
                "description": description,
                "published_at": published_at,
                "view_count": view_count,
                "like_count": like_count,
            }

        except Exception as e:
            bt.logging.warning(f"_get_video_metadata failed for {video_id}: {e}")
            return None

    async def _scrape_single_video(self, youtube_url: str, language: Optional[str] = None) -> List[DataEntity]:
        """
        Single-video scrape via the Apify invideoiq/video-transcript-scraper actor.
        If `language` is provided, try that first; otherwise let the actor choose best available.
        Falls back to API for authoritative upload date.
        """
        try:
            # 1) Resolve video id
            video_id = youtube_utils.extract_video_id_from_url(youtube_url)
            if not video_id:
                bt.logging.error(f"Invalid YouTube URL: {youtube_url}")
                return []

            # 2) Try language-specific first (if requested), else any-language
            transcript_data = None
            if language:
                transcript_data = await self._get_transcript_from_actor(video_id, language)
            if not transcript_data:
                transcript_data = await self._get_transcript_from_actor_any_language(video_id)
            if not transcript_data:
                bt.logging.error(f"No data returned for video {video_id}")
                return []

            transcript = transcript_data.get("transcript", [])

            if not transcript_data:
                bt.logging.error(f"No transcript data returned for video {video_id}")
                return []
            
            # 3) Language: prefer explicit param; else detect; else actor’s selected; else default
            lang = (
                (language or "").strip()
                or youtube_utils.detect_transcript_language(transcript)
                or transcript_data.get("selected_language")
                or self.DEFAULT_LANGUAGE
            )

            # 4) Title/channel from actor; upload date from YT API for accuracy
            metadata = await self._get_single_video_metadata(video_id)
            title = metadata.get("title")
            description = metadata.get("description")
            upload_date = metadata.get("upload_date")
            transcript_data["title"] = title
            transcript_data["description"] = description
            transcript_data['view_count'] = metadata.get('view_count', transcript_data.get('view_count'))
            transcript_data['like_count'] = metadata.get('like_count')
            
            if not upload_date:
                bt.logging.warning(f"No upload date for video {video_id}, using current time")
                upload_date = dt.datetime.now(dt.timezone.utc)

            # Get subscriber count
            channel_id = transcript_data.get('channel_id')
            if channel_id:
                subscriber_count = await self._get_channel_subscriber_count(channel_id)
                transcript_data['subscriber_count'] = subscriber_count
                
            # Create content
            content = self._create_youtube_content(
                video_id=video_id,
                language=lang,
                upload_date=upload_date,
                transcript_data=transcript_data,
            )

            # Convert to DataEntity
            entity = YouTubeContent.to_data_entity(content)

            bt.logging.success(f"Successfully scraped video {video_id} using invideoiq actor")
            return [entity]

        except Exception as e:
            bt.logging.error(f"Error scraping video {video_id}: {str(e)}")
            return []

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate YouTube transcript entities using unified validation function."""
        if not entities:
            return []

        results = []

        for entity in entities:
            try:
                content_to_validate = YouTubeContent.from_data_entity(entity)
                original_language = content_to_validate.language

                bt.logging.info(
                    f"Validating video {content_to_validate.video_id} in original language: {original_language}")

                # Get upload date from YouTube API for enhanced timestamp validation
                real_upload_date = await self._get_upload_date_from_api(content_to_validate.video_id)
                if not real_upload_date:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Cannot verify upload date from YouTube API - potential timestamp manipulation",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Get raw actor payload
                transcript_data = await self._get_transcript_from_actor(content_to_validate.video_id, original_language)
                if not transcript_data:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Video transcript not available in original language",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Validate view count (minimum engagement check)
                view_count = transcript_data.get('view_count', 0)
                if int(view_count) < 100:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Video has low engagement ({view_count} views, minimum 100 required)",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Get additional data from API for validation
                statistics = await self._get_video_statistics_from_api(content_to_validate.video_id)
                if statistics:
                    transcript_data['view_count'] = statistics.get('view_count', transcript_data.get('view_count'))
                    transcript_data['like_count'] = statistics.get('like_count')

                # Get subscriber count from API
                channel_id = transcript_data.get('channel_id')
                subscriber_count = None
                if channel_id:
                    subscriber_count = await self._get_channel_subscriber_count(channel_id)

                # Create YouTubeContent from actor payload (same logic as scrape method)
                actual_youtube_content = YouTubeContent(
                    video_id=transcript_data.get('video_id', content_to_validate.video_id),
                    title=transcript_data.get('title', ''),
                    channel_name=transcript_data.get('channel', ''),
                    upload_date=real_upload_date,  # Use API timestamp for accuracy
                    transcript=transcript_data.get('transcript', []),
                    url=f"https://www.youtube.com/watch?v={content_to_validate.video_id}",
                    duration_seconds=int(transcript_data.get('duration', 0)),
                    language=original_language,
                    description=transcript_data.get('description'),
                    thumbnails=youtube_utils.generate_thumbnails(content_to_validate.video_id),
                    view_count=transcript_data.get('view_count'),
                    like_count=transcript_data.get('like_count'),
                    subscriber_count=subscriber_count
                )

                # Convert to DataEntity (labels created automatically)
                actual_entity = YouTubeContent.to_data_entity(actual_youtube_content)

                # Use unified validation function
                validation_result = youtube_utils.validate_youtube_data_entities(
                    entity_to_validate=entity,
                    actual_entity=actual_entity
                )
                results.append(validation_result)

            except Exception as e:
                bt.logging.error(f"Validation error: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation error: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

        return results

    def _validate_content_match(self, actual_data: Dict[str, Any], stored_content: YouTubeContent,
                                entity: DataEntity) -> ValidationResult:
        """Validate that stored content matches actual data."""

        # Validate video ID (should be exact match)
        actual_video_id = actual_data.get('video_id', '')
        if actual_video_id and actual_video_id != stored_content.video_id:
            return ValidationResult(
                is_valid=False,
                reason="Video ID mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate title (allow minor differences)
        if not self._texts_are_similar(actual_data.get('title', ''), stored_content.title, threshold=0.8):
            return ValidationResult(
                is_valid=False,
                reason="Title mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate transcript content
        actual_transcript = actual_data.get('transcript', [])
        if not self._transcripts_are_similar(actual_transcript, stored_content.transcript):
            return ValidationResult(
                is_valid=False,
                reason="Transcript content mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate language match
        actual_language = actual_data.get('selected_language', '')
        expected_language = stored_content.language
        # if expected_language and actual_language != expected_language:
        #     return ValidationResult(
        #         is_valid=False,
        #         reason=f"Language mismatch: expected {expected_language}, got {actual_language}",
        #         content_size_bytes_validated=entity.content_size_bytes
        #     )

        return ValidationResult(
            is_valid=True,
            reason="Content validated successfully",
            content_size_bytes_validated=entity.content_size_bytes
        )

    def _extract_transcript_text(self, transcript: List[Dict]) -> str:
        """Extract full text from transcript."""
        if not transcript:
            return ""
        return " ".join([item.get('text', '') for item in transcript])

    def _texts_are_similar(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Check if two texts are similar enough."""
        if not text1 or not text2:
            return text1 == text2

        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 or not words2:
            return False

        overlap = len(words1.intersection(words2))
        similarity = overlap / max(len(words1), len(words2))

        return similarity >= threshold

    def _transcripts_are_similar(self, transcript1: List[Dict], transcript2: List[Dict],
                                 threshold: float = 0.7) -> bool:
        """Check if two transcripts are similar enough."""
        text1 = self._extract_transcript_text(transcript1)
        text2 = self._extract_transcript_text(transcript2)
        return self._texts_are_similar(text1, text2, threshold)

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts."""
        if not text1 or not text2:
            return 0.0

        words1 = set(re.sub(r'[^\w\s]', '', text1.lower()).split())
        words2 = set(re.sub(r'[^\w\s]', '', text2.lower()).split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def _is_within_date_range(self, date: dt.datetime, date_range: DateRange) -> bool:
        """Check if a date is within the specified date range."""
        return date_range.contains(date)

    async def get_video_metadata(self, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Batch-fetch YouTube metadata for the given video IDs.
        Returns:
        {
            <video_id>: {
            "title": str,
            "published_at": datetime | None,   # tz-aware UTC
            "duration_seconds": int,
            },
            ...
        }
        """
        # de-duplicate and sanitize
        unique_ids: List[str] = sorted({video_id for video_id in video_ids if isinstance(video_id, str) and video_id.strip()})
        if not unique_ids:
            return {}

        # pick an API key: prefer primary scraper's, else fallback's
        api_key: Optional[str] = os.getenv("YOUTUBE_METADATA_API_KEY", None) or os.getenv("YOUTUBE_API_KEY", None)
        if not api_key:
            bt.logging.warning("get_video_metadata: no YouTube API key available.")
            return {}

        base_url: str = "https://www.googleapis.com/youtube/v3/videos"
        results: Dict[str, Dict[str, Any]] = {}
        chunk_size: int = 50  # API max per request
        timeout = httpx.Timeout(10.0)

        async with httpx.AsyncClient(timeout=timeout) as client:
            for start in range(0, len(unique_ids), chunk_size):
                batch_ids = unique_ids[start:start + chunk_size]
                params = {
                    "id": ",".join(batch_ids),
                    "part": "snippet,contentDetails",  # statistics removed to save quota
                    "key": api_key,
                }
                try:
                    response = await client.get(base_url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                except Exception as e:
                    bt.logging.warning(f"get_video_metadata: request failed for batch starting with {batch_ids[0]}: {e}")
                    continue

                for item in payload.get("items", []):
                    video_id = item.get("id")
                    if not video_id:
                        continue

                    snippet = item.get("snippet", {}) or {}
                    content_details = item.get("contentDetails", {}) or {}

                    title: str = snippet.get("title", "") or ""
                    channel_name: str = snippet.get("channelTitle", "") or ""

                    published_at_str: str = (snippet.get("publishedAt") or "").replace("Z", "+00:00")
                    try:
                        published_at = dt.datetime.fromisoformat(published_at_str) if published_at_str else None
                        if published_at and published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=dt.timezone.utc)
                    except Exception:
                        published_at = None

                    try:
                        duration_seconds: int = int(isodate.parse_duration(content_details.get("duration") or "PT0S").total_seconds())
                    except Exception:
                        duration_seconds = 0

                    results[video_id] = {
                        "title": title,
                        "channel_name": channel_name,
                        "upload_date": published_at,
                        "duration_seconds": duration_seconds,
                    }

        return results


# Test functions
async def test_channel_scrape():
    """Test channel-based scraping functionality."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING CHANNEL SCRAPE TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeChannelTranscriptScraper()

    test_labels = [
        DataLabel(value="#ytc_c_leon-kuwata"),
    ]

    bt.logging.info(f"Testing with labels: {[label.value for label in test_labels]}")

    scrape_config = ScrapeConfig(
        entity_limit=3,
        date_range=DateRange(
            start=dt.datetime(2025, 9, 5, tzinfo=dt.timezone.utc),
            end=dt.datetime.now(dt.timezone.utc)
        ),
        labels=test_labels
    )

    bt.logging.info(f"Scrape config - Entity limit: {scrape_config.entity_limit}")
    bt.logging.info(f"Date range: {scrape_config.date_range.start} to {scrape_config.date_range.end}")

    bt.logging.info("Starting scrape process...")
    entities, status = await scraper.scrape(scrape_config)

    bt.logging.success(f"Scrape completed! Found {len(entities)} entities from channels")

    if entities:
        bt.logging.info("=" * 40)
        bt.logging.info("SCRAPED ENTITIES DETAILS:")
        bt.logging.info("=" * 40)

        for i, entity in enumerate(entities, 1):
            content = YouTubeContent.from_data_entity(entity)
            bt.logging.info(f"Entity {i}:")
            bt.logging.info(f"  Title: {content.title}")
            bt.logging.info(f"  Video ID: {content.video_id}")
            bt.logging.info(f"  Label: {entity.label.value}")
            bt.logging.info(f"  Language: {content.language}")
            bt.logging.info(f"  Upload Date: {content.upload_date}")
            bt.logging.info(f"  Duration: {content.duration_seconds}s")
            bt.logging.info(f"  Description in transcript: {content.description}")
            bt.logging.info(f"  Transcript segments: {len(content.transcript)}")
            bt.logging.info(f"  Subscriber count: {content.subscriber_count}")
            bt.logging.info(f"  View count: {content.view_count}")
            bt.logging.info(f"  Like count: {content.like_count}")
            bt.logging.info(f"  Content size: {entity.content_size_bytes} bytes")
            bt.logging.info(f"  URL: {content.url}")
            if content.transcript:
                first_transcript = content.transcript[0]
                bt.logging.info(f"  First transcript: {first_transcript}")
            bt.logging.info("-" * 40)
    else:
        bt.logging.warning("No entities were scraped!")

    bt.logging.info("CHANNEL SCRAPE TEST COMPLETED")
    return entities


async def test_validation():
    """Test validation functionality."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING VALIDATION TEST")
    bt.logging.info("=" * 60)

    bt.logging.info("First, scraping some entities to validate...")
    entities = await test_channel_scrape()

    if not entities:
        bt.logging.error("❌ No entities scraped - cannot proceed with validation test")
        return

    bt.logging.info(f"Got {len(entities)} entities from scrape, proceeding with validation...")

    scraper = YouTubeChannelTranscriptScraper()

    bt.logging.info("Starting validation process...")
    bt.logging.info(f"Validating {len(entities)} entities...")

    for i, entity in enumerate(entities, 1):
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(
            f"Entity {i}: Video {content.video_id} ({content.title[:30]}...) in language {content.language}")

    results = await scraper.validate(entities)

    bt.logging.info(f"Validation completed! Got {len(results)} results")

    bt.logging.info("=" * 40)
    bt.logging.info("VALIDATION RESULTS:")
    bt.logging.info("=" * 40)

    valid_count = 0
    invalid_count = 0

    for i, result in enumerate(results, 1):
        entity = entities[i - 1]
        content = YouTubeContent.from_data_entity(entity)

        if result.is_valid:
            valid_count += 1
            bt.logging.success(f"✅ Entity {i}: VALID")
        else:
            invalid_count += 1
            bt.logging.error(f"❌ Entity {i}: INVALID")

        bt.logging.info(f"   Video: {content.video_id} ({content.title[:40]}...)")
        bt.logging.info(f"   Language: {content.language}")
        bt.logging.info(f"   Reason: {result.reason}")
        bt.logging.info(f"   Bytes validated: {result.content_size_bytes_validated}")
        bt.logging.info("-" * 30)

    bt.logging.info("VALIDATION SUMMARY:")
    bt.logging.success(f"✅ Valid: {valid_count}/{len(results)} ({valid_count / len(results) * 100:.1f}%)")
    bt.logging.warning(f"❌ Invalid: {invalid_count}/{len(results)} ({invalid_count / len(results) * 100:.1f}%)")

    bt.logging.info("VALIDATION TEST COMPLETED")
    return results


async def test_get_transcript():
    # a small(< 9MB output) non-en video
    # video_id = 'LfCYDBOdaN4'
    # language = 'th'

    # a long(> 9MB output) non-en video
    video_id = 'RfSr5sdFlhU'
    language = 'hi'

    scraper = YouTubeChannelTranscriptScraper()
    transcript_data = await scraper._get_transcript_from_actor(video_id, language)
    bt.logging.debug(transcript_data)
    result = True if transcript_data else False
    return result


async def test_single_video_scrape():
    video_id = '9DI8g7bBn5s'
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    language = 'hi'

    scraper = YouTubeChannelTranscriptScraper()
    entities = await scraper._scrape_single_video(video_url, language)
    entity = entities[0]

    content = YouTubeContent.from_data_entity(entity)
    bt.logging.info(f"  Title: {content.title}")
    bt.logging.info(f"  Channel: {content.channel_name}")
    bt.logging.info(f"  Video ID: {content.video_id}")
    bt.logging.info(f"  Label: {entity.label.value}")
    bt.logging.info(f"  Language: {content.language}")
    bt.logging.info(f"  Upload Date: {content.upload_date}")
    bt.logging.info(f"  Duration: {content.duration_seconds}s")
    bt.logging.info(f"  Description in transcript: {content.description}")
    bt.logging.info(f"  Transcript segments: {len(content.transcript)}")
    bt.logging.info(f"  Subscriber count: {content.subscriber_count}")
    bt.logging.info(f"  View count: {content.view_count}")
    bt.logging.info(f"  Like count: {content.like_count}")
    bt.logging.info(f"  Content size: {entity.content_size_bytes} bytes")
    bt.logging.info(f"  URL: {content.url}")
    if content.transcript:
        first_transcript = content.transcript[0]
        bt.logging.info(f"  First transcript: {first_transcript}")
    bt.logging.info("-" * 40)

    return entity


async def main():
    """Main test function."""
    print("\nFinal YouTube Channel-Based Transcript Scraper")
    print("=" * 50)
    print("1. Test channel scraping")
    print("2. Test validation")
    print("3. Test full pipeline")
    print("4. Test get transcript")
    print("5. Test scrape single video")
    print("6. Exit")

    choice = input("\nEnter your choice (1-5): ")

    if choice == "1":
        await test_channel_scrape()
    elif choice == "2":
        await test_validation()
    elif choice == "3":
        bt.logging.info("Running full pipeline test...")
        await test_channel_scrape()
        await test_validation()
    elif choice == "4":
        bt.logging.info("Running get transcript test...")
        await test_get_transcript()
        return
    elif choice == "5":
        await test_single_video_scrape()
    elif choice == "6":
        print("Exiting.")
        return
    else:
        print("Invalid choice.")
        await main()


if __name__ == "__main__":
    setup_bt_logging()
    bt.logging.set_trace(True)
    bt.logging.info("Starting Final YouTube Channel Transcript Scraper tests...")
    asyncio.run(main())