# src/pydpiper_shell/core/plugins/modules/crawl_report_plugin.py
import logging
from collections import Counter
import pandas as pd # Make sure pandas is imported

from pydpiper_shell.core.plugins.base import PluginBase
from pydpiper_shell.core.plugins.facade import PluginFacade

logger = logging.getLogger(__name__)

class CrawlReportPlugin(PluginBase):
    """
    A plugin that generates a performance report based on data queried from the database.
    """

    def run(self, app: PluginFacade, args: list[str]) -> int:
        """
        Loads data via the facade (using SQL), analyzes, and reports.
        """
        try:
            app.logger.info("Crawl Report Plugin: Loading data via facade...")
            # Load DataFrames using the updated facade methods
            requests_df = app.cache.load_requests_df()
            pages_df = app.cache.load_pages_df()
            project = app.cache.load_project() # Still loads the Project model

            # Check if DataFrames are empty or project loading failed
            if requests_df.empty or project is None:
                print("⚠️  No request data found or project metadata missing. Cannot generate report.")
                # Check if pages_df is also empty for a more specific message
                if pages_df.empty:
                     print("   (Also no page data found).")
                return 1

            # --- Calculations using DataFrames ---
            total_requests = len(requests_df)
            total_pages = len(pages_df) # Count rows in the pages DataFrame

            # Total duration logic (using project or request timestamps)
            total_duration_sec = 1.0 # Default fallback
            if project.total_time is not None and project.total_time > 0:
                total_duration_sec = project.total_time
            elif not requests_df.empty and 'created_at' in requests_df.columns:
                app.logger.warning("project.total_time not set, calculating from request timestamps.")
                try:
                    # Convert 'created_at' (likely ISO strings from DB) to datetime objects
                    requests_df['created_at_dt'] = pd.to_datetime(requests_df['created_at'], errors='coerce')
                    if requests_df['created_at_dt'].notna().any(): # Check if any timestamps were valid
                         start_time = requests_df['created_at_dt'].min().timestamp()
                         end_time = requests_df['created_at_dt'].max().timestamp()
                         calculated_duration = end_time - start_time
                         total_duration_sec = max(calculated_duration, 1.0) # Ensure at least 1 second
                    else:
                         app.logger.error("Could not parse 'created_at' timestamps in requests table.")
                except Exception as time_calc_err:
                     app.logger.error(f"Error calculating duration from timestamps: {time_calc_err}")
            else:
                 app.logger.warning("Could not determine crawl duration (no project.total_time and no request timestamps). Using default 1s.")


            # Performance metrics
            pages_per_sec = round(total_pages / total_duration_sec, 2) if total_duration_sec > 0 else 0
            sec_per_page = round(total_duration_sec / total_pages, 2) if total_pages > 0 else 0

            # --- CORRECTED Average page size calculation using DataFrame ---
            # Filter DataFrame for rows where 'content' is not null/NaN
            pages_with_content_df = pages_df.dropna(subset=['content'])
            if not pages_with_content_df.empty:
                 # Calculate byte length directly on the 'content' column
                 # Ensure content is treated as string, encode to utf-8, get length, then sum
                 try:
                      total_size_bytes = pages_with_content_df['content'].astype(str).str.encode('utf-8').str.len().sum()
                      avg_size_kb = round((total_size_bytes / len(pages_with_content_df)) / 1024, 2)
                 except Exception as size_err:
                      app.logger.error(f"Error calculating average page size: {size_err}")
                      avg_size_kb = 0.0 # Fallback on error
            else:
                 avg_size_kb = 0.0
            # --- END CORRECTION ---

            # Status code distribution using DataFrame's value_counts
            if not requests_df.empty and 'status_code' in requests_df.columns:
                 # Use value_counts() for efficient counting, convert index (codes) to int
                 status_counts = requests_df['status_code'].value_counts()
                 status_distribution = {int(code): count for code, count in status_counts.items()}
            else:
                 status_distribution = Counter() # Empty counter if no request data


            # --- Reporting (remains the same structure) ---
            print("\n" + "—" * 40)
            print("🕸 Crawl Performance Report")
            print(f"   Project ID: {app.project_id}")
            print("—" * 40)
            print(f"📊 Total HTTP Requests: {total_requests}")
            print(f"📄 Total Pages Crawled: {total_pages}")
            print(f"⏱️ Total Duration: {total_duration_sec:.2f} seconds")
            print("")
            print(f"🚀 Pages per Second: {pages_per_sec}")
            print(f"⌛ Seconds per Page: {sec_per_page}")
            print(f"💾 Average Page Size: {avg_size_kb} KB")
            print("")
            print("🚦 Status Code Distribution:")
            if status_distribution:
                for code, count in sorted(status_distribution.items()):
                    print(f"   - Code {code}: {count} times")
            else:
                print("   (No request data available)")
            print("—" * 40 + "\n")

            return 0

        except Exception as e:
            app.logger.error("Error generating the crawl report: %s", e, exc_info=True)
            print(f"❌ Error: Could not generate report. Details: {e}")
            return 1