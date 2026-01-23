# src/pydpiper_shell/core/handlers/audit_handler.py
import argparse
import logging
import multiprocessing
import time
from typing import Dict, Any, Optional

import pandas as pd
from tqdm.auto import tqdm

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.path_utils import PathUtils
from pydpiper_shell.core.managers.database_manager import DatabaseManager

# Import controllers & services
from auditor.controllers.audit_controller import AuditController
from auditor.managers.audit_ignore_manager import AuditIgnoreManager
from auditor.services.pagerank_service import PageRankService
from auditor.utils.launcher import launch_report_server_detached
from crawler.services.data_prepare_service import DataPrepareService
from pydpiper_shell.core.services.dataframe_service import DataFrameService

# ---------------------

logger = logging.getLogger(__name__)

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "run": None,
    "rank": None,
    "report": None,
    "config": None,
}


def handle_audit(args: list[str], ctx: ShellContext) -> int:
    """
    Handler for audit commands.
    """
    project_id_str = ctx.get("project.id")
    if not project_id_str:
        print("❌ No project loaded. Please select a project first.")
        return 1

    project_id = int(project_id_str)

    parser = argparse.ArgumentParser(prog="audit")
    subparsers = parser.add_subparsers(dest="subcommand", help="Audit subcommands")

    # 1. Subcommand: CONFIG
    config_parser = subparsers.add_parser("config", help="Configure audit rules (ignore lists)")
    config_parser.add_argument("--imgignore", type=str, help="Add images to ignore.")
    config_parser.add_argument("--imgignore-reset", action="store_true", help="Reset image ignore list.")
    config_parser.add_argument("--imgignore-list", action="store_true", help="Show image ignore list.")
    config_parser.add_argument("--linkignore", type=str, help="Add links to ignore.")
    config_parser.add_argument("--linkignore-reset", action="store_true", help="Reset link ignore list.")
    config_parser.add_argument("--linkignore-list", action="store_true", help="Show link ignore list.")

    # 2. Subcommand: RUN
    run_parser = subparsers.add_parser("run", help="Run the audit process")
    run_parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of pages.")
    run_parser.add_argument("--url", type=str, default=None, help="Filter by URL.")
    run_parser.add_argument("--export", type=str, default=None, help="Save flat results to Excel.")
    run_parser.add_argument("--workers", type=int, default=max(1, multiprocessing.cpu_count() - 1), help="CPU workers")

    # 3. Subcommand: REPORT
    subparsers.add_parser("report", help="Start the interactive audit report server")

    # 4. Subcommand: RANK
    subparsers.add_parser("rank", help="Calculate Internal PageRank (IPR)")

    try:
        if not args:
            parser.print_help()
            return 0

        if args[0].startswith("-"):
            args.insert(0, 'run')

        if args[0] not in ['run', 'report', 'config', 'rank']:
            args.insert(0, 'run')

        parsed_args = parser.parse_args(args)
    except SystemExit:
        return 1

    if parsed_args.subcommand == "config":
        return _handle_config(parsed_args, project_id)
    elif parsed_args.subcommand == "report":
        return _handle_report(parsed_args, ctx, project_id)
    elif parsed_args.subcommand == "rank":
        return _handle_rank(ctx, project_id)
    elif parsed_args.subcommand == "run":
        return _handle_run(parsed_args, ctx, project_id)

    return 0


def _handle_config(parsed_args: argparse.Namespace, project_id: int) -> int:
    cache_path = PathUtils.get_cache_root()
    ignore_manager = AuditIgnoreManager(project_id, cache_path)

    def print_ignore_list(label, items):
        print(f"\n📋 Ignored {label}s:")
        print("-" * 40)
        if not items: print("  (List is empty)")
        for item in sorted(items): print(f"  - {item}")
        print("-" * 40)

    updated_img = ignore_manager.update_ignore_list('img', parsed_args.imgignore, parsed_args.imgignore_reset)
    if parsed_args.imgignore_list: print_ignore_list("Image", ignore_manager.ignored_images)

    updated_link = ignore_manager.update_ignore_list('link', parsed_args.linkignore, parsed_args.linkignore_reset)
    if parsed_args.linkignore_list: print_ignore_list("Link", ignore_manager.ignored_links)

    return 0


def _handle_report(parsed_args: argparse.Namespace, ctx: ShellContext, project_id: int) -> int:
    db_path = PathUtils.get_project_db_path(project_id)
    if launch_report_server_detached(project_id, db_path):
        return 0
    return 1


def _handle_rank(ctx: ShellContext, project_id: int) -> int:
    """Calculates IPR scores with precision timing."""
    print(f"🧮 Calculating Internal PageRank for Project {project_id}...")

    try:
        db = ctx.db_manager if hasattr(ctx, 'db_manager') else DatabaseManager()
        if hasattr(db, 'init_db_schema'):
            db.init_db_schema(project_id)

        service = PageRankService(project_id, db)

        # --- HIGH PRECISION TIMER ---
        start_time = time.perf_counter()
        count = service.calculate_and_save()
        duration_ms = (time.perf_counter() - start_time) * 1000

        if count > 0:
            print(f"✅ IPR calculated and saved for {count} pages in {duration_ms:.2f}ms.")
            print("   You can now view the Optimization Potential in 'audit report'.")
        else:
            print(f"⚠️  Calculation finished in {duration_ms:.2f}ms but no pages updated.")

    except Exception as e:
        print(f"❌ Error calculating rank: {e}")
        return 1
    return 0


def _handle_run(parsed_args: argparse.Namespace, ctx: ShellContext, project_id: int) -> int:
    """Executes audit with performance tracking."""
    cache_path = PathUtils.get_cache_root()
    ignore_manager = AuditIgnoreManager(project_id, cache_path)
    dps = DataPrepareService()
    dfs = DataFrameService()

    print(f"🚀 Starting Audit for Project {project_id}...")

    try:
        dbm = ctx.db_manager if hasattr(ctx, 'db_manager') else DatabaseManager()
        pages = dfs.fetch_dataframe(project_id, "SELECT * FROM pages")

        if hasattr(dbm, 'get_connection'):
            conn = dbm.get_connection(project_id)
            try:
                req_df = pd.read_sql_query("SELECT url, status_code FROM requests", conn)
                status_map = pd.Series(req_df.status_code.values, index=req_df.url).to_dict()
                print(f"   Mapped {len(status_map)} known URLs.")
            except:
                status_map = {}
        else:
            status_map = {}
    except Exception as e:
        print(f"❌ Could not load data: {e}")
        return 1

    if pages.empty:
        print("No pages found in database.")
        return 0

    if parsed_args.url:
        pages = pages[pages['url'].astype(str).str.contains(parsed_args.url, case=False, na=False)]
    if parsed_args.max_pages:
        pages = pages.head(parsed_args.max_pages)

    print(f"🚀 Analyzing {len(pages)} pages using {parsed_args.workers} cores...")

    controller = AuditController(project_id, ignore_manager)
    pbar = tqdm(total=len(pages), desc="Auditing", unit="page")

    def progress_update(current, total):
        pbar.n = current
        pbar.refresh()

    # --- AUDIT TIMER START ---
    start_audit = time.perf_counter()
    summary = controller.run_audit(
        pages,
        status_map=status_map,
        workers=parsed_args.workers,
        progress_callback=progress_update
    )
    audit_duration = time.perf_counter() - start_audit
    pbar.close()

    # Inject duration into summary
    summary['duration'] = audit_duration

    # Save Issues
    issues = controller.get_results_for_db()
    if issues:
        sql, tuples = dps.prepare_audit_issues(issues)
        if sql: dbm.save_batch(project_id, sql, tuples)
        print(f"\n💾 {len(issues)} findings processed and saved.")

    # Save Classifications
    page_types = controller.get_page_types_for_db()
    if page_types:
        sql, tuples = dps.prepare_page_elements(page_types)
        if sql: dbm.save_batch(project_id, sql, tuples)
        print(f"📦 {len(page_types)} page classifications saved.")

    _print_summary(summary)

    if parsed_args.export:
        _handle_export(parsed_args.export, controller.get_results_for_export())

    return 0


def _print_summary(summary):
    stats = summary.get('stats', {})
    duration = summary.get('duration', 0)

    print("\n" + "=" * 60)
    print("📊 AUDIT SUMMARY")
    print("=" * 60)
    print(f"Pages Analyzed:      {summary.get('total_pages', 0)}")
    print(f"Total Issues Found:  {summary.get('total_issues', 0)}")
    print(f"Analysis Duration:   {duration:.2f} seconds")
    print("-" * 60)

    if stats:
        print(f"{'CATEGORY':<15} | {'ISSUE CODE':<35} | {'COUNT':>5}")
        print("-" * 60)
        for cat in sorted(stats.keys()):
            sub = stats[cat]
            items = sub.most_common() if hasattr(sub, 'most_common') else sub.items()
            for code, count in items:
                print(f"{cat:<15} | {code:<35} | {count:>5}")
    print("=" * 60 + "\n")


def _handle_export(filename, data):
    if not data: return
    try:
        if not filename.endswith(".xlsx"): filename += ".xlsx"
        out_path = PathUtils.get_user_documents_dir() / filename
        pd.DataFrame(data).to_excel(out_path, index=False)
        print(f"✅ Report exported to: {out_path}")
    except Exception as e:
        print(f"❌ Error exporting: {e}")