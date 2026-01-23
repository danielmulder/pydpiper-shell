# src/pydpiper_shell/core/plugins/modules/email_scraper_plugin.py
import logging
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from typing import Optional, List, Set, Dict, Any
from pathlib import Path
from urllib.parse import urlparse
import argparse
from datetime import datetime

from pydpiper_shell.core.plugins.base import PluginBase
from pydpiper_shell.core.plugins.facade import PluginFacade
from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)


class EmailScraperPlugin(PluginBase):
    """
    Scans all 'pages' of the active project, extracts email addresses, and
    saves the unique list to an Excel file.

    Defaults: append=True, scope=True, dynamic output filename.
    """

    EMAIL_REGEX = re.compile(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    )

    def _scrape_emails_from_html(self, html_content: str) -> Set[str]:
        """Extracts email addresses from a single piece of HTML."""
        found_emails: Set[str] = set()
        if not html_content:
            return found_emails
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Method 1: Search for 'mailto:' links
            for a in soup.select('a[href^="mailto:"]'):
                href = a.get('href', '')
                email = href.replace('mailto:', '', 1).split('?')[0].strip()
                if email and "." in email.split("@")[-1]:
                    found_emails.add(email.lower())

            # Method 2: De-obfuscate text and use regex
            page_text = soup.get_text(separator=" ")
            deobfuscated_text = page_text.replace(" [at] ", "@").replace(" (at) ", "@")
            deobfuscated_text = deobfuscated_text.replace(" [dot] ", ".").replace(" (dot) ", ".")
            for match in self.EMAIL_REGEX.finditer(deobfuscated_text):
                email = match.group(0).strip()
                found_emails.add(email.lower())
        except Exception:
            pass
        return found_emails

    def run(self, app: PluginFacade, args: list[str]) -> int:

        # --- 0. Parse Arguments (With flags to DISABLE defaults) ---
        parser = argparse.ArgumentParser(prog="email_scraper")
        parser.add_argument("--output-file", type=str, default=None, help="Output filename.")
        parser.add_argument("--no-append", action="store_true", help="Disable appending (force overwrite).")
        parser.add_argument("--no-scope", action="store_true", help="Disable domain scoping.")

        try:
            # We use parse_known_args to ignore the rest of the args
            pargs = parser.parse_known_args(args)[0]
        except SystemExit:
            return 1

        # Determine logical defaults: IF the flag is NOT provided, the feature is ON.
        SHOULD_APPEND = not pargs.no_append
        SHOULD_SCOPE = not pargs.no_scope

        # --- 1. Validate Project and Setup ---
        if not app.project_id or app.project_id == 0 or not app.cache:
            print("❌ Error: No project loaded.")
            return 1

        project_id = app.project_id
        project_naam = app.ctx.get("project.name") or f"project_{project_id}"
        start_url_str = app.ctx.get("project.start_url") or ""
        app.logger.info(f"Starting email_scraper_plugin for project: {project_naam} (ID: {project_id})")

        # --- 2. Load URLs from Database ---
        try:
            pages_df = app.cache.load_pages_df()
            if pages_df.empty:
                print(f"⚠️ No pages found in the database for project {project_id}.")
                return 1

            print(f"📄 {len(pages_df)} pages loaded from project '{project_naam}'. Starting scan...")
        except Exception as e:
            app.logger.error(f"Error reading 'pages' table: {e}", exc_info=True)
            print(f"❌ Error reading the database: {e}")
            return 1

        # --- 3. Scraping and Filtering ---
        all_emails_in_project: Set[str] = set()

        # 3a. Scraping
        for html_content in tqdm(pages_df['content'].dropna(), desc="Scanning pages", total=len(pages_df)):
            all_emails_in_project.update(self._scrape_emails_from_html(html_content))

        # 3b. Filter by Domain (Scoped Emails)
        emails_to_save: Set[str] = set()
        if all_emails_in_project:
            if SHOULD_SCOPE and start_url_str:
                try:
                    project_domain = urlparse(start_url_str).netloc.removeprefix("www.")
                    emails_to_save = {email for email in all_emails_in_project if
                                      email.split("@")[-1] == project_domain}
                except Exception:
                    emails_to_save = all_emails_in_project
            else:
                emails_to_save = all_emails_in_project

        if not emails_to_save:
            print(f"✅ Scanning complete. No domain-scoped email addresses found.")
            return 0

        # --- 4. Save Results (Consolidated/Forced Append) ---
        output_dir = PathUtils.get_user_documents_dir()

        # Determine filename and output path
        if pargs.output_file:
            output_file = Path(pargs.output_file)
            if not output_file.is_absolute():
                output_file = output_dir / output_file
        else:
            # Default filename: email_scraper_output_{timestamp}
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_dir / f"email_scraper_output_{timestamp}.xlsx"

        # Create the DataFrame to save
        df_new = pd.DataFrame({
            "project_name": project_naam,
            "project_domain": urlparse(start_url_str).netloc if start_url_str else project_naam,
            "email_address": sorted(list(emails_to_save))
        })

        # Append logic (Now without 'mode' argument)
        if SHOULD_APPEND and output_file.exists():
            try:
                # Read existing file, concat, and overwrite
                df_existing = pd.read_excel(output_file, engine='openpyxl')
                # Keep unique rows, based on domains and addresses
                df_combined = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(
                    subset=['project_domain', 'email_address'], keep='first')

                # to_excel overwrites by default now.
                df_combined.to_excel(output_file, index=False, engine='openpyxl')

                print(
                    f"\n✅ Scanning complete! {len(emails_to_save)} domain-scoped email addresses added to (Append Mode):")
            except Exception as e:
                app.logger.error(f"Error consolidating/appending the Excel file: {e}", exc_info=True)

                # Fallback to overwriting ONLY the new data
                df_new.to_excel(output_file, index=False, engine='openpyxl')
                print(
                    f"\n❌ Error modifying existing Excel file. New data saved in an emergency file:")
        else:
            # Standard write mode (overwrite, or unique timestamp)
            df_new.to_excel(output_file, index=False, engine='openpyxl')
            print(
                f"\n✅ Scanning complete! {len(emails_to_save)} domain-scoped email addresses saved in (Write Mode):")

        print(f"   {output_file}")
        return 0