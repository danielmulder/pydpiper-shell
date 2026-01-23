import logging
import math
import networkx as nx
import pandas as pd
from typing import Dict
from collections import defaultdict

from pydpiper_shell.core.managers.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class PageRankService:
    """
    Calculates the relative importance of pages within a site's internal link structure.
    Utilizes the PageRank algorithm on a directed graph of internal URLs.
    """

    def __init__(self, project_id: int, db_manager: DatabaseManager):
        self.project_id = project_id
        self.db_manager = db_manager
        self.damping_factor = 0.85  # Probability of a user continuing to click links
        self.max_iter = 100  # Maximum iterations for convergence
        self.tolerance = 1e-6  # Convergence threshold

    def calculate_and_save(self) -> int:
        """
        Calculates Internal PageRank (IPR) and persists it to the database.

        This method normalizes URLs (e.g., stripping trailing slashes) to merge
        duplicate nodes during calculation, then broadcasts the resulting score
        to all matching URL variations found in the pages table.
        """
        conn = self.db_manager.get_connection(self.project_id)

        # 1. Fetch Internal Links
        logger.info("Loading link graph from database...")
        try:
            # We only calculate PageRank for internal links to determine internal authority
            links_df = pd.read_sql_query(
                "SELECT source_url, target_url FROM links WHERE is_external = 0",
                conn
            )
        except Exception as e:
            logger.error(f"Error loading links: {e}")
            return 0

        if links_df.empty:
            logger.warning("No internal links found for PageRank calculation.")
            return 0

        # --- STEP 1.5: URL NORMALIZATION ---
        # Treat 'site.com/page' and 'site.com/page/' as the same node to avoid score splitting
        logger.info("Normalizing URLs (merging trailing slashes)...")
        links_df['source_url'] = links_df['source_url'].apply(self._normalize_url)
        links_df['target_url'] = links_df['target_url'].apply(self._normalize_url)

        # 2. Construct the Directed Graph
        graph = nx.from_pandas_edgelist(
            links_df,
            source='source_url',
            target='target_url',
            create_using=nx.DiGraph()
        )

        logger.info(f"Calculating PageRank for {graph.number_of_nodes()} unique nodes...")

        # 3. Calculate PageRank using NetworkX
        try:
            pr_scores = nx.pagerank(
                graph,
                alpha=self.damping_factor,
                max_iter=self.max_iter,
                tol=self.tolerance
            )
        except Exception as e:
            logger.error(f"PageRank convergence failed: {e}")
            return 0

        # 4. Normalize Scores to a Logarithmic 0-10 Scale
        # Standard PageRank values are tiny decimals; log scaling makes them human-readable
        scores_to_save_normalized = self._normalize_scores_logarithmic(pr_scores)

        # 5. Mapping and Persistence
        # We map normalized keys back to every actual URL variation in the DB
        cursor = conn.cursor()
        logger.info("Mapping calculated scores back to database URLs...")

        try:
            all_pages_df = pd.read_sql_query("SELECT url FROM pages", conn)

            # Map normalized_url -> list of actual_db_urls
            # e.g. 'kuras.nl/contact' -> ['kuras.nl/contact', 'kuras.nl/contact/']
            url_map = defaultdict(list)
            for real_url in all_pages_df['url']:
                norm = self._normalize_url(real_url)
                url_map[norm].append(real_url)

            # Build batch update data
            data_tuples = []
            for norm_url, score in scores_to_save_normalized.items():
                real_urls = url_map.get(norm_url, [])
                for real_url in real_urls:
                    data_tuples.append((score, real_url))

            if not data_tuples:
                logger.warning("No matching URLs found in 'pages' table to update.")
                return 0

            logger.info(f"Saving IPR scores to {len(data_tuples)} rows in database...")

            cursor.executemany("UPDATE pages SET ipr = ? WHERE url = ?", data_tuples)
            conn.commit()

            updated_rows = cursor.rowcount
            logger.info(f"✅ Successfully updated {updated_rows} pages (merged variations).")
            return updated_rows

        except Exception as e:
            logger.error(f"Error saving IPR scores: {e}")
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def _normalize_url(self, url: str) -> str:
        """Strips trailing slashes for consistent node identification."""
        if not url:
            return ""
        return url.rstrip('/')

    def _normalize_scores_logarithmic(self, scores: Dict[str, float]) -> Dict[str, float]:
        """
        Transforms raw PageRank decimals into a 0-10 scale.
        Uses a logarithmic transformation to ensure a clear distribution
        between high-authority and low-authority pages.
        """
        raw_values = [s for s in scores.values() if s > 0]
        if not raw_values:
            return {}

        # Apply log transformation with a small epsilon to avoid log(0)
        log_scores = {url: math.log(score + 1e-20) for url, score in scores.items()}

        log_values = list(log_scores.values())
        min_log = min(log_values)
        max_log = max(log_values)
        range_val = max_log - min_log

        transformed = {}
        for url, l_score in log_scores.items():
            if range_val < 1e-9:
                transformed[url] = 5.0
            else:
                # Linear normalization of log values to 0-10 range
                norm = (l_score - min_log) / range_val
                transformed[url] = round(norm * 10.0, 4)

        return transformed