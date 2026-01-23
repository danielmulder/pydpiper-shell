# src/pydpiper_shell/core/plugins/modules/external_link_checker_plugin.py
import asyncio
import logging
import time
from urllib.parse import urlparse, urlunparse
from collections import defaultdict
from typing import Dict, Optional, List, Set

from tqdm.asyncio import tqdm
import pandas as pd

from pydpiper_shell.core.plugins.base import PluginBase
from pydpiper_shell.core.plugins.facade import PluginFacade
from crawler.services.http_request_service import HttpRequestService
from crawler.utils.url_utils import UrlUtils
from crawler.services.generate_default_user_agent_service import generate_default_user_agent

logger = logging.getLogger(__name__)


class ExternalLinkCheckerPlugin(PluginBase):
    """
    External link checker utilizing the centralized HttpRequestService.
    Features:
    - Adaptive throttling per domain.
    - HEAD request optimization.
    - Deduplication via URL normalization.
    - Optimized DB writing (Unique Target Update Strategy).
    """

    def __init__(self):
        self.max_retries = 2
        self.default_domain_concurrency = 5
        self.backoff_factor_429 = 2.0

        self.domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self.domain_delays: Dict[str, float] = {}
        self.http_service: Optional[HttpRequestService] = None

    @staticmethod
    def _get_normalized_url(url: str) -> str:
        try:
            parsed = urlparse(url)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        except Exception:
            return url

    async def check_link_worker(self, url: str) -> dict:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
        except Exception:
            return {"url": url, "status_code": -1, "error": "Invalid URL format"}

        logger.debug("Checking url %s.'.", url)

        sem = self.domain_semaphores.get(domain, asyncio.Semaphore(1))
        base_delay = self.domain_delays.get(domain, 0.0)

        result = {"url": url, "status_code": 0, "error": None}

        for attempt in range(self.max_retries + 1):
            async with sem:
                current_delay = base_delay * (self.backoff_factor_429 ** attempt) if attempt > 0 else base_delay
                if current_delay > 0:
                    await asyncio.sleep(current_delay)

                if not self.http_service:
                    return {"url": url, "status_code": -1, "error": "Service not initialized"}

                fetch_result = await self.http_service.perform_request(url, method="HEAD")
                status = fetch_result.get("status", -99)

                if status in [429, 503]:
                    logger.debug("Url %s returned status: '%i'.", url, status)
                    new_delay = max(self.domain_delays.get(domain, 0.5) * 2, 1.0)
                    self.domain_delays[domain] = new_delay
                    if attempt < self.max_retries:
                        continue

                if status == 405:
                    fetch_result = await self.http_service.perform_request(url, method="GET")
                    status = fetch_result.get("status", -99)

                result["status_code"] = status
                if status < 0:
                    result["error"] = fetch_result.get("error")

                return result

        result["error"] = "Max retries exceeded"
        return result

    def run(self, app: PluginFacade, args: list[str]) -> int:
        # 1. Setup Configuration
        # We laden nu specifiek de 'link_checker' sectie
        link_checker_config = app.get_config("link_checker", {})

        # Defaults als de config ontbreekt
        lc_concurrency = int(link_checker_config.get('concurrency', 100))
        lc_timeout = int(link_checker_config.get('timeout', 15))
        lc_retries = int(link_checker_config.get('retries', 2))

        # Update interne settings
        self.max_retries = lc_retries

        # Configureer de service met de SPECIFIEKE link checker settings
        service_config = {
            "session": {
                "concurrency": lc_concurrency,
                "time_out": lc_timeout,
                "max_redirects": 5  # Hardcoded voor check, of ook uit config halen
            }
        }

        app.logger.info(f"Initializing Link Checker with concurrency={lc_concurrency}, timeout={lc_timeout}s")

        user_agent = generate_default_user_agent()
        self.http_service = HttpRequestService(service_config, UrlUtils(), user_agent)

        # 2. Load Data
        app.logger.info("Loading external links...")
        try:
            if hasattr(app.cache, 'load_external_links_df'):
                try:
                    df_links = app.cache.load_external_links_df(app.project_id)
                except TypeError:
                    df_links = app.cache.load_external_links_df()
            else:
                df_links = app.cache.load_generic_json_df("external_links")
        except Exception as e:
            print(f"❌ Error loading links: {e}")
            return 1

        if df_links.empty:
            print("✅ No external links to check.")
            return 0

        if 'status_code' in df_links.columns:
            df_links['status_code'] = pd.to_numeric(df_links['status_code'], errors='coerce').fillna(0).astype(int)
            to_check = df_links[df_links['status_code'] == 0]
        else:
            to_check = df_links

        # --- NORMALIZATION & MAPPING ---
        raw_urls = [u for u in to_check['target_url'].tolist() if u and u.startswith('http')]

        # Map: Normalized URL -> List of Original URLs
        url_map: Dict[str, List[str]] = defaultdict(list)
        for original_url in raw_urls:
            norm_url = self._get_normalized_url(original_url)
            url_map[norm_url].append(original_url)

        unique_normalized_urls = list(url_map.keys())

        if not unique_normalized_urls:
            print("✅ All valid external links are already checked.")
            return 0

        print(
            f"🚀 Optimization: Checking {len(unique_normalized_urls)} normalized URLs (mapped from {len(raw_urls)} links)...")

        # 3. Setup Domain Semaphores
        domain_counts = defaultdict(int)
        for url in unique_normalized_urls:
            try:
                domain_counts[urlparse(url).netloc] += 1
            except Exception:
                pass

        for domain, count in domain_counts.items():
            limit = self.default_domain_concurrency
            if count > 50: limit = max(1, int(limit * 0.5))
            self.domain_semaphores[domain] = asyncio.Semaphore(limit)
            self.domain_delays[domain] = 0.0

        # 4. Run Async
        async def runner():
            async with self.http_service:
                tasks = [self.check_link_worker(url) for url in unique_normalized_urls]
                results = []
                for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Checking", unit="link"):
                    results.append(await f)
                return results

        try:
            results = asyncio.run(runner())
        except Exception as e:
            app.logger.error(f"Runner failed: {e}", exc_info=True)
            print(f"❌ Runner Error: {e}")
            return 1

        # 5. Save to DB (Optimized)
        print("💾 Propagating and saving results...")
        prop_start_time = time.perf_counter()

        # --- OPTIMIZATIE: Dedupliceer op TARGET URL niveau ---
        # We maken een map: target_url -> status
        # Hierdoor krijgt de DB slechts 1 update commando per UNIEKE string in de database.
        updates_by_url: Dict[str, int] = {}

        for res in results:
            norm_url = res['url']
            status = res['status_code']

            if status != 0:
                # Haal alle originele URLs op die bij deze normalized URL horen
                original_urls = url_map.get(norm_url, [])

                # Voeg ze toe aan de updates map.
                # Omdat dit een dict is, worden dubbele strings automatisch ontdubbeld.
                for orig_url in original_urls:
                    updates_by_url[orig_url] = status

        # Maak tuples voor de DB
        update_tuples = [(status, app.project_id, url) for url, status in updates_by_url.items()]

        if update_tuples:
            try:
                # Batch updates om SQLite transacties klein te houden
                batch_size = 5000
                total_updated = 0

                # Gebruik db_mgr
                conn = app.ctx.db_mgr.get_connection(app.project_id)

                with conn:
                    for i in range(0, len(update_tuples), batch_size):
                        batch = update_tuples[i:i + batch_size]
                        conn.executemany(
                            "UPDATE links SET status_code = ? WHERE project_id = ? AND target_url = ? AND is_external = 1",
                            batch
                        )
                        total_updated += len(batch)

                prop_duration = time.perf_counter() - prop_start_time
                print(
                    f"✅ Updated {total_updated} unique link targets (affecting all matching rows) in {prop_duration:.2f}s.")

            except Exception as e:
                print(f"❌ Database Error: {e}")
                return 1
        else:
            print("⚠️ No link statuses were updated.")

        return 0