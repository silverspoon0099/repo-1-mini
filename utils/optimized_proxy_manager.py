import asyncio
import itertools
import random
import os
from datetime import datetime, timedelta
from typing import Optional
from functools import lru_cache
import bittensor as bt
from dotenv import load_dotenv

load_dotenv()


@lru_cache(maxsize=1)
def load_iproyal_proxies() -> list[str]:
    username = os.getenv("IPROYAL_PROXY_USERNAME")
    password = os.getenv("IPROYAL_PROXY_PASSWORD")

    if not username or not password:
        bt.logging.warning("⚠️ Missing IPRoyal proxy credentials in environment.")
        return []

    countries = "us,at,au,be,ch,cz,de,dk,es,fi,fr,gb,gr,hu,ie,is,it,lt,lv,mt,nl,no,pl,pt,ro,se,si".split(
        ","
    )
    proxies = [
        f"http://{username}:{password}_country-{country}_streaming-1@geo.iproyal.com:12321"
        for country in countries
    ]

    if not proxies:
        bt.logging.warning("⚠️ No proxies generated. Scraper may face rate limits.")

    return proxies


@lru_cache(maxsize=1)
def load_proxyshare_proxies() -> list[str]:
    username = os.getenv("PROXY_SHARE_USERNAME")
    password = os.getenv("PROXY_SHARE_PASSWORD")

    if not username or not password:
        bt.logging.warning("⚠️ Missing ProxyShare proxy credentials in environment.")
        return []

    countries = "us,at,au,be,ch,cz,de,dk,es,fi,fr,gb,gr,hu,ie,is,it,lt,lv,mt,nl,no,pl,pt,ro,se,si".split(
        ","
    )
    proxies = [
        f"http://{username}_area-{country}:{password}@proxy.proxyshare.com:5959"
        for country in countries
    ]

    if not proxies:
        bt.logging.warning("⚠️ No proxies generated. Scraper may face rate limits.")

    return proxies


@lru_cache(maxsize=1)
def load_proxies() -> list[str]:
    proxies = []

    proxies.extend(load_iproyal_proxies())
    proxies.extend(load_proxyshare_proxies())

    if not proxies:
        bt.logging.warning("⚠️ No proxies generated. Scraper may face rate limits.")

    random.shuffle(proxies)

    return proxies


class OptimizedProxyManager:
    def __init__(self, proxy_list: Optional[list[str]] = None):
        self.proxies = proxy_list or load_proxies()
        self.failed_proxies = {}
        self.last_used = {}
        self._lock = asyncio.Lock()
        self.failure_threshold = 300  # seconds
        self.reuse_delay = 2  # seconds
        self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None

    async def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None

        async with self._lock:
            now = datetime.now()
            self.failed_proxies = {
                p: t
                for p, t in self.failed_proxies.items()
                if (now - t).total_seconds() < self.failure_threshold
            }

            for _ in range(min(len(self.proxies), 10)):
                proxy = next(self.proxy_cycle)

                if proxy in self.failed_proxies:
                    continue

                if (
                    proxy in self.last_used
                    and (now - self.last_used[proxy]).total_seconds() < self.reuse_delay
                ):
                    continue

                self.last_used[proxy] = now
                return proxy

            if len(self.failed_proxies) > len(self.proxies) * 0.7:
                self.failed_proxies.clear()
                proxy = next(self.proxy_cycle)
                self.last_used[proxy] = now
                return proxy

            return None

    async def mark_failed(self, proxy: str):
        if proxy:
            async with self._lock:
                self.failed_proxies[proxy] = datetime.now()
                
class ProxyAssignmentManager:
    def __init__(self, proxy_list: Optional[list[str]] = None):
        self.proxies = proxy_list or load_proxies()
        self.assignments = {}         # index → list of proxy candidates
        self.failed_proxies = set()   # proxies marked failed
        self.current_proxy = {}       # index → proxy currently in use

    def get_proxy_for_index(self, index: int) -> Optional[str]:
        if index in self.current_proxy:
            proxy = self.current_proxy[index]
            if proxy not in self.failed_proxies:
                return proxy  # Reuse if still valid

        if index not in self.assignments:
            shuffled = self.proxies.copy()
            random.shuffle(shuffled)
            self.assignments[index] = shuffled

        # Try from assigned list
        for proxy in self.assignments[index]:
            if proxy not in self.failed_proxies and proxy not in self.current_proxy.values():
                self.current_proxy[index] = proxy
                return proxy

        # If all proxies are exhausted, reset and try once more
        bt.logging.warning(f"🔁 All proxies failed or in use for index {index}. Resetting failures.")
        self.reset_all_failures()
        for proxy in self.assignments[index]:
            if proxy not in self.current_proxy.values():
                self.current_proxy[index] = proxy
                return proxy

        bt.logging.warning(f"❗ Still no available proxy after reset for index {index}")
        return None

    def mark_failed(self, proxy: str):
        self.failed_proxies.add(proxy)
        # Remove from current_proxy if used
        for idx, p in list(self.current_proxy.items()):
            if p == proxy:
                del self.current_proxy[idx]

    def reset_failure(self, proxy: str):
        self.failed_proxies.discard(proxy)

    def reset_all_failures(self):
        self.failed_proxies.clear()
        
@lru_cache(maxsize=1)
def load_web_share_proxy(mode: str = "residential", idx: Optional[int] = None) -> Optional[str]:
    proxy_username = os.getenv("WEB_SHARE_PROXY_USERNAME")
    proxy_password = os.getenv("WEB_SHARE_PROXY_PASSWORD")
    domain_name = os.getenv("WEB_SHARE_PROXY_DOMAIN_NAME")
    proxy_port = os.getenv("WEB_SHARE_PROXY_PORT")
    region_env = os.getenv("WEB_SHARE_PROXY_REGION")

    if not all([proxy_username, proxy_password, domain_name, proxy_port]):
        return None  # Missing env vars

    if mode == "residential":
        region_suffix = ""
        if region_env:
            regions = [r.strip().upper() for r in region_env.split(',') if r.strip()]
            region_suffix = "-" + "-".join(regions) if regions else ""
        proxy = f"http://{proxy_username}-{mode}{region_suffix}-rotate:{proxy_password}@{domain_name}:{proxy_port}"
        return proxy

    elif mode == "staticresidential":
        if idx is None:
            return None
        proxy = f"http://{proxy_username}-{mode}-{idx}:{proxy_password}@{domain_name}:{proxy_port}"
        return proxy

    return None