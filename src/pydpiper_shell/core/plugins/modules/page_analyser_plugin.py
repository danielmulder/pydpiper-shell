# ============================================
# file: src/pydpiper_shell/core/plugins/modules/page_analyser_plugin.py
# ============================================
from __future__ import annotations

import logging
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from pydpiper_shell.core.plugins.base import PluginBase
from pydpiper_shell.core.plugins.facade import PluginFacade
from crawler.model import PageMetric

logger = logging.getLogger(__name__)
tqdm.pandas()


class PageAnalyzerPlugin(PluginBase):
    """
    Analyzes crawl data (pages, page_elements, links, images) and writes
    aggregated metrics to the 'plugin_page_metrics' table.
    """

    # ---------- Helpers ----------

    @staticmethod
    def _count_words(text: Optional[str]) -> int:
        """Counts the number of words in a given text string."""
        if pd.isna(text) or not isinstance(text, str) or not text:
            return 0
        return len(text.split())

    @staticmethod
    def _pivot_elements(elements_df: pd.DataFrame, index: pd.Index) -> pd.DataFrame:
        """
        Pivots the page_elements DataFrame.

        Expected columns: page_id, element_type, content.
        The content might already be parsed into an object/dict by the facade.
        """
        if elements_df.empty:
            return pd.DataFrame(index=index)
        try:
            # Use pivot_table with aggfunc='first' to handle duplicates
            pv = pd.pivot_table(
                elements_df,
                index="page_id",
                columns="element_type",
                values="content",
                aggfunc='first'
            )
            pv.columns.name = None
            # Ensure all page_ids are present
            return pv.reindex(index=index)
        except Exception as e:
            logger.error("Error pivoting elements: %s", e, exc_info=True)
            return pd.DataFrame(index=index)

    @staticmethod
    def _aggregate_images_from_elements(master_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates total_images, missing_alt_tags, and missing_alt_ratio
        assuming 'images' is stored as an element (list[dict] per page).
        """
        def calc(images_value) -> pd.Series:
            if not isinstance(images_value, list) or not images_value:
                return pd.Series(
                    [0, 0, 0.0],
                    index=["total_images", "missing_alt_tags", "missing_alt_ratio"]
                )

            total = 0  # Start count at 0
            missing = 0

            for img in images_value:
                if isinstance(img, dict):
                    # Filter 1x1 transparent gif data-uris
                    src = str(img.get("src", "") or img.get("image_url", "")).strip()
                    if src.startswith("data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///"):
                        continue  # Skip this image entirely

                    total += 1  # Count only valid images
                    alt_txt = str(img.get("alt", "") or img.get("alt_text", "")).strip()
                    if not alt_txt:
                        missing += 1

            # Determine ratio
            ratio = (missing / total) * 100 if total > 0 else 0.0
            return pd.Series(
                [total, missing, round(ratio, 2)],
                index=["total_images", "missing_alt_tags", "missing_alt_ratio"]
            )

        # Apply to the 'images' column if it exists
        if "images" in master_df.columns:
            im = master_df["images"].progress_apply(calc)
            return im
        else:
            # Return DataFrame with correct structure but zeros if 'images' column is missing
            return pd.DataFrame(
                0,
                index=master_df.index,
                columns=["total_images", "missing_alt_tags", "missing_alt_ratio"]
            )

    @staticmethod
    def _aggregate_images_from_table(images_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates image metrics per page_id from a dedicated images table.
        Table structure expected: page_id, image_url, alt_text, width, height.
        """
        cols = ["page_id", "total_images", "missing_alt_tags", "missing_alt_ratio"]
        if images_df.empty:
            return pd.DataFrame(columns=cols).set_index("page_id")

        # Filter 1x1 transparent gif (data-uri)
        mask_data_uri = images_df["image_url"].astype(str).str.startswith(
            "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///"
        )
        df = images_df.loc[~mask_data_uri].copy()

        if df.empty:  # If only data URIs remained
            return pd.DataFrame(columns=cols).set_index("page_id")

        df["has_alt"] = df["alt_text"].fillna("").astype(str).str.strip().ne("")

        agg = df.groupby("page_id").agg(
            total_images=("image_url", "count"),
            missing_alt_tags=("has_alt", lambda s: int((~s).sum())),
        )

        # Prevent division by zero if there are no images after filtering
        agg["missing_alt_ratio"] = np.where(
            agg["total_images"] > 0,
            (agg["missing_alt_tags"] / agg["total_images"]) * 100.0,
            0.0
        )
        agg["missing_alt_ratio"] = agg["missing_alt_ratio"].round(2)

        return agg

    # ---------- Main Execution ----------

    def run(self, app: PluginFacade, args: List[str]) -> int:
        try:
            app.logger.info("Page Analyzer Plugin: Loading project data via facade...")

            # 1) Load Pages
            pages_df = app.cache.load_pages_df()
            if pages_df.empty:
                print("⚠️  No pages found in database. Run 'crawler run' first.")
                return 1
            if "id" not in pages_df.columns:
                app.logger.error("Column 'id' missing in pages table.")
                print("❌ Error: Invalid pages schema (missing 'id').")
                return 1

            pages_df = pages_df.rename(columns={"id": "page_id"})
            pages_df["url"] = pages_df["url"].astype(str)

            # Keep 'url' in pages_df_base for later merge
            pages_df_base = pages_df[["page_id", "url", "content"]].set_index("page_id")

            # 2) Load Elements
            app.logger.info("Loading parsed elements from 'page_elements' table...")
            needed_elements = [
                "page_title",
                "meta_description",
                "canonical_tag",
                "headings",
                "images"  # Ensure 'images' is here for fallback
            ]
            elements_df = app.cache.load_page_elements_df(element_types=needed_elements)
            pivoted_elements = self._pivot_elements(elements_df, index=pages_df_base.index)

            # 3) Load and Aggregate Link Counts
            app.logger.info("Loading and aggregating link counts...")
            internal_links_df = app.cache.load_internal_links_df()
            external_links_df = app.cache.load_external_links_df()

            # Use pages_df directly (has page_id and url)
            link_counts_temp_df = pages_df[["page_id", "url"]].copy()

            if not internal_links_df.empty:
                internal_counts = internal_links_df.groupby("source_url").size().rename("internal_link_count")
                incoming_counts = internal_links_df.groupby("target_url").size().rename("incoming_link_count")
                link_counts_temp_df = link_counts_temp_df.merge(
                    internal_counts, left_on="url", right_index=True, how="left"
                )
                link_counts_temp_df = link_counts_temp_df.merge(
                    incoming_counts, left_on="url", right_index=True, how="left"
                )
            else:
                link_counts_temp_df["internal_link_count"] = 0
                link_counts_temp_df["incoming_link_count"] = 0

            if not external_links_df.empty:
                external_counts = external_links_df.groupby("source_url").size().rename("external_link_count")
                link_counts_temp_df = link_counts_temp_df.merge(
                    external_counts, left_on="url", right_index=True, how="left"
                )
            else:
                link_counts_temp_df["external_link_count"] = 0

            count_cols = ["internal_link_count", "external_link_count", "incoming_link_count"]
            link_counts_temp_df[count_cols] = link_counts_temp_df[count_cols].fillna(0).astype(int)
            # Set page_id as index for the final join
            link_counts_df = link_counts_temp_df.set_index("page_id")[count_cols]

            # 4) Load Image Data
            # Try separate table first; if not, fallback to elements['images']
            app.logger.info("Loading image data (table or elements)...")
            img_metrics_df: pd.DataFrame
            images_df: Optional[pd.DataFrame] = None  # Start with None

            try:
                # Always attempt to call load_images_df
                images_df = app.cache.load_images_df()
            except AttributeError:
                app.logger.warning(
                    "Method 'load_images_df' not found on facade.cache. Skipping direct image table load."
                )
                images_df = None
            except Exception as e_img_load:
                app.logger.error("Error loading images_df: %s", e_img_load, exc_info=True)
                images_df = None

            if images_df is not None and not images_df.empty:
                app.logger.info("Aggregating image metrics from 'images' table.")
                # Aggregate from table (agg already has page_id index)
                img_metrics_df = self._aggregate_images_from_table(images_df)
            else:
                # Fallback: From elements column (list[dict] per page)
                app.logger.info(
                    "Attempting to aggregate image metrics from 'page_elements' (images column)."
                )
                # Ensure pivoted_elements has an 'images' column
                if "images" not in pivoted_elements.columns:
                    pivoted_elements["images"] = None

                tmp_for_img_agg = pages_df_base.join(
                    pivoted_elements[['images']], how="left"
                )

                if "images" in tmp_for_img_agg.columns:
                    im_agg_result = self._aggregate_images_from_elements(tmp_for_img_agg)
                    img_metrics_df = im_agg_result
                else:
                    app.logger.warning(
                        "Could not find 'images' column even for fallback. Setting image metrics to zero."
                    )
                    img_metrics_df = pd.DataFrame(
                        0,
                        index=pages_df_base.index,
                        columns=["total_images", "missing_alt_tags", "missing_alt_ratio"]
                    )

            # ---- Fetch server_time (elapsed_time) (FIRST MATCH VERSION) ----
            app.logger.info("Loading request timings for server_time (first 200 match)...")
            requests_df = app.cache.load_requests_df()
            # Create DataFrame with page_id index directly
            server_times_df = pd.DataFrame(index=pages_df_base.index)
            server_times_df['server_time'] = None  # Initialize with None

            if not requests_df.empty and 'elapsed_time' in requests_df.columns:
                # Select only successful (200) requests and relevant columns
                requests_200 = requests_df.loc[
                    requests_df['status_code'] == 200, ['url', 'elapsed_time']
                ].copy()

                if not requests_200.empty:
                    # Remove duplicates (keep first if URL crawled multiple times)
                    requests_200.drop_duplicates(subset=['url'], keep='first', inplace=True)

                    # Merge with pages_df (containing page_id)
                    merged_times = pd.merge(
                        pages_df[['page_id', 'url']],
                        requests_200,
                        on='url',
                        how='left'  # Keep all pages
                    ).set_index('page_id')

                    # Assign found times to server_times_df
                    server_times_df['server_time'] = merged_times['elapsed_time']
                else:
                    app.logger.warning(
                        "No successful (status 200) requests found to calculate server_time."
                    )
            else:
                app.logger.warning("Requests data is empty or missing 'elapsed_time' column.")
            # ---- END server_time ----

            # 5) Merge All DataFrames
            app.logger.info(
                "Merging base pages, elements, image metrics, link counts, and server times..."
            )
            master_df = pages_df_base  # index: page_id, contains 'url', 'content'
            master_df = master_df.join(pivoted_elements, how="left")
            master_df = master_df.join(link_counts_df, how="left")
            master_df = master_df.join(img_metrics_df, how="left")
            master_df = master_df.join(server_times_df[['server_time']], how='left')

            # Reset index to make page_id a column again for subsequent logic
            master_df = master_df.reset_index()

            defaults = [
                ("total_images", 0),
                ("missing_alt_tags", 0),
                ("missing_alt_ratio", 0.0),
                ("internal_link_count", 0),
                ("external_link_count", 0),
                ("incoming_link_count", 0),
                ("server_time", None),  # Default is None if no match found
            ]

            for col, default_val in defaults:
                if col in master_df.columns:
                    # Only call fillna if default_val is NOT None
                    if default_val is not None:
                        master_df[col] = master_df[col].fillna(default_val)
                    # If default_val is None, do nothing; NaN remains NaN
                else:
                    # Add column if missing entirely
                    master_df[col] = default_val

            # 6) Calculate Metrics
            app.logger.info("Calculating metrics...")
            metrics = pd.DataFrame(index=master_df.index)
            metrics["page_id"] = master_df["page_id"]
            metrics["project_id"] = app.project_id
            metrics["url"] = master_df["url"]

            metrics["title_length"] = (
                master_df.get("page_title", pd.Series(dtype="str"))
                .fillna("").astype(str).str.len()
            )

            def h1_len(head) -> int:
                """Calculates the length of the first h1 tag found."""
                if isinstance(head, dict) and head.get("h1"):
                    h1_list = head["h1"]
                    if isinstance(h1_list, list) and h1_list:
                        try:
                            # Use length of the *first* element
                            return len(str(h1_list[0]))
                        except Exception:
                            return 0
                return 0

            metrics["h1_length"] = (
                master_df.get("headings", pd.Series(dtype="object")).apply(h1_len)
            )

            metrics["meta_desc_length"] = (
                master_df.get("meta_description", pd.Series(dtype="str"))
                .fillna("").astype(str).str.len()
            )

            # Image metrics from merges (fillna already done)
            metrics["total_images"] = pd.to_numeric(
                master_df["total_images"], errors="coerce"
            ).fillna(0).astype(int)

            metrics["missing_alt_tags"] = pd.to_numeric(
                master_df["missing_alt_tags"], errors="coerce"
            ).fillna(0).astype(int)

            metrics["missing_alt_ratio"] = pd.to_numeric(
                master_df["missing_alt_ratio"], errors="coerce"
            ).fillna(0.0)

            # Link counts (fillna already done)
            metrics["internal_link_count"] = master_df["internal_link_count"].astype(int)
            metrics["external_link_count"] = master_df["external_link_count"].astype(int)
            metrics["incoming_link_count"] = master_df["incoming_link_count"].astype(int)

            # Canonical present?
            metrics["has_canonical"] = (
                master_df.get("canonical_tag", pd.Series(dtype="str")).notna().astype(int)
            )

            # Word count
            app.logger.info("Calculating word counts...")
            metrics["word_count"] = (
                master_df.get("content", pd.Series(dtype="str"))
                .progress_apply(self._count_words)
            )

            # Server time (fillna already done)
            metrics["server_time"] = master_df["server_time"]
            metrics["broken_img_ratio"] = None  # As requested

            # Align columns with PageMetric model
            final_cols = list(PageMetric.model_fields.keys())
            for col in final_cols:
                if col not in metrics.columns:
                    # Improved default logic based on PageMetric model
                    field_info = PageMetric.model_fields.get(col)
                    default_value = None  # Start with None
                    if field_info:
                        if field_info.default is not None:
                            default_value = field_info.default
                        # Based on type hint
                        elif field_info.annotation == int:
                            default_value = 0
                        elif field_info.annotation == float:
                            default_value = 0.0
                        elif field_info.annotation == str:
                            default_value = ""
                        elif field_info.annotation == list:
                            default_value = []
                        elif field_info.annotation == dict:
                            default_value = {}
                    metrics[col] = default_value

            metrics = metrics[final_cols]  # Select desired columns in correct order

            # Type Enforcement
            type_map = PageMetric.model_fields
            for col, field_info in type_map.items():
                if col not in metrics.columns:
                    continue

                target_type = field_info.annotation
                is_optional = False

                # Check if it's an Optional type (Union[T, None])
                if hasattr(target_type, '__origin__') and target_type.__origin__ is Union:
                    args = getattr(target_type, '__args__', ())
                    if len(args) == 2 and type(None) in args:
                        is_optional = True
                        # Find the non-None type
                        target_type = next(t for t in args if t is not type(None))

                try:
                    current_series = metrics[col]
                    # Handle None/NaN specifically for optional fields
                    if is_optional:
                        if target_type in (int, float):
                            casted_series = pd.to_numeric(current_series, errors='coerce')
                        else:
                            casted_series = current_series

                        # Convert NaN back to None
                        metrics[col] = np.where(
                            casted_series.isna(),
                            None,
                            casted_series.astype(target_type, errors='ignore')
                        )
                    # Non-optional fields
                    elif target_type is int:
                        metrics[col] = pd.to_numeric(
                            current_series, errors='coerce'
                        ).fillna(0).astype(int)
                    elif target_type is float:
                        metrics[col] = pd.to_numeric(current_series, errors='coerce')
                    elif target_type is str:
                        metrics[col] = current_series.fillna("").astype(str)

                except Exception as te:
                    app.logger.warning(
                        f"Type cast failed for column '{col}' to '{target_type}': {te}. "
                        f"Column data sample:\n{metrics[col].head()}"
                    )

            # Final replacement of Pandas/Numpy nulls to Python None
            metrics_final = metrics.replace({pd.NA: None, np.nan: None, pd.NaT: None})

            # 7) Save Results
            table_name = "page_metrics"
            app.logger.info(
                "Analysis complete. Saving %d rows to '%s' table...",
                len(metrics_final),
                f"plugin_{table_name}"
            )
            # Use save_dataframe which adds the 'plugin_' prefix
            app.cache.save_dataframe(table_name, metrics_final, if_exists="replace")

            print(
                f"\n✅ Successfully analyzed and saved metrics for {len(metrics_final)} pages."
            )
            print(
                f"   Results saved to table: 'plugin_{table_name}'"
            )
            return 0

        except Exception as e:
            app.logger.error(
                "Error during page analysis plugin execution: %s", e, exc_info=True
            )
            print(f"❌ Error: Could not complete analysis. Details: {e}")
            return 1