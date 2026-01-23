# src/crawler/services/request_to_link_status_propagator_service.py
# (De class PropagationGraph code blijft hetzelfde als voorheen, hieronder de aangepaste Service class)
import asyncio
import time
import logging
from collections import defaultdict, deque
from typing import Dict, Set, Any

import pandas as pd
from crawler.managers.crawl_data_manager import CrawlDataManager

logger = logging.getLogger(__name__)


class PropagationGraph:
    def __init__(self) -> None:
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.statuses: Dict[str, int] = {}

    def add_link(self, source_url: str, target_url: str) -> None:
        self.graph[source_url].add(target_url)

    def set_status(self, url: str, status: Any) -> None:
        status_int = 0
        if isinstance(status, (int, float)) and pd.notna(status):
            try:
                status_int = int(status)
            except (ValueError, TypeError):
                pass
        if status_int <= 0:
            if url not in self.statuses:
                self.statuses[url] = 0
            return
        existing_status = self.statuses.get(url, 0)
        if status_int > existing_status:
            self.statuses[url] = status_int

    def propagate_statuses(self) -> None:
        queue = deque([u for u, s in self.statuses.items() if s > 0])
        in_queue = set(queue)
        iterations = 0
        max_iterations = len(self.graph) * 2 + 1000
        while queue and iterations < max_iterations:
            iterations += 1
            current_url = queue.popleft()
            in_queue.discard(current_url)
            current_status = self.statuses.get(current_url, 0)
            if current_status == 0: continue
            targets = self.graph.get(current_url, set())
            for target_url in targets:
                existing_target_status = self.statuses.get(target_url, 0)
                if current_status > existing_target_status:
                    self.statuses[target_url] = current_status
                    if target_url not in in_queue:
                        queue.append(target_url)
                        in_queue.add(target_url)

    def get_all_statuses(self) -> Dict[str, int]:
        return dict(self.statuses)


class RequestToLinkStatusPropagatorService:
    def __init__(self, project_id: int, db_facade: CrawlDataManager) -> None:
        self.project_id = project_id
        self.db_facade = db_facade

    async def run(self) -> dict:
        logger.info("Starting in-memory link status propagation for project %s...", self.project_id)
        total_start_time = time.perf_counter()
        loop = asyncio.get_running_loop()

        # 1. Load Data into DataFrames via Facade
        try:
            def load_dfs():
                links = self.db_facade.load_internal_links_df(self.project_id)
                reqs = self.db_facade.load_requests_df(self.project_id)
                pages = self.db_facade.load_pages_df(self.project_id)
                return links, reqs, pages

            internal_links_df, requests_df, pages_df = await loop.run_in_executor(None, load_dfs)

        except Exception as e:
            logger.error("Failed to load dataframes: %s", e, exc_info=True)
            return {"error": str(e)}

        if internal_links_df.empty:
            logger.info("No internal links to propagate.")
            return {"updated": 0}

        # 2. Build Graph
        graph = PropagationGraph()
        logger.debug("Building graph from %d links...", len(internal_links_df))

        for row in internal_links_df.itertuples(index=False):
            src = getattr(row, 'source_url', None)
            tgt = getattr(row, 'target_url', None)
            if src and tgt:
                graph.add_link(str(src), str(tgt))

        # 3. Set Initial Statuses
        count_seeds = 0
        # From Requests
        if not requests_df.empty and 'url' in requests_df.columns and 'status_code' in requests_df.columns:
            unique_reqs = requests_df.drop_duplicates(subset=['url'], keep='last')
            for row in unique_reqs.itertuples(index=False):
                url = getattr(row, 'url', None)
                status = getattr(row, 'status_code', 0)
                if url:
                    graph.set_status(str(url), status)
                    count_seeds += 1

        # From Pages (fallback)
        if not pages_df.empty and 'url' in pages_df.columns and 'status_code' in pages_df.columns:
            unique_pages = pages_df.drop_duplicates(subset=['url'], keep='last')
            for row in unique_pages.itertuples(index=False):
                url = getattr(row, 'url', None)
                status = getattr(row, 'status_code', 0)
                if url:
                    graph.set_status(str(url), status)

        logger.debug("Graph seeded with %d status codes.", count_seeds)

        # 4. Propagate (MET TIMER)
        logger.debug("Starting graph propagation algorithm...")
        prop_start_time = time.perf_counter()

        graph.propagate_statuses()

        prop_end_time = time.perf_counter()
        prop_duration = prop_end_time - prop_start_time
        logger.info("Graph propagation algorithm finished in %.4fs", prop_duration)

        # 5. Persist Results
        final_statuses = graph.get_all_statuses()
        update_tuples = []

        for url, status in final_statuses.items():
            if status > 0:
                update_tuples.append((status, self.project_id, url))

        updated_count = 0
        if update_tuples:
            logger.info("Writing %d propagated statuses to DB...", len(update_tuples))

            def execute_update():
                conn = self.db_facade.delegate.get_connection(self.project_id)
                with conn:
                    conn.executemany(
                        "UPDATE links SET status_code = ? WHERE project_id = ? AND target_url = ?",
                        update_tuples
                    )
                return len(update_tuples)

            updated_count = await loop.run_in_executor(None, execute_update)

        total_duration = time.perf_counter() - total_start_time
        logger.info("Propagation complete. Updated %d links in %.2fs.", updated_count, total_duration)

        return {
            "time_total_ms": round(total_duration * 1000, 2),
            "time_graph_prop_ms": round(prop_duration * 1000, 2),  # Specifieke tijd voor het algoritme
            "updated_links": updated_count
        }