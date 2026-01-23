# ============================================
# file: src/pydpiper_shell/core/handlers/query_handler.py
# ============================================
import argparse
import logging
import os
import platform
import subprocess
import re
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
import pandas as pd
import json

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.services.query_service import QueryService
from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)

query_help_text = """
QUERY:
  query <subcommand> [options]

Subcommands:
  run "<query_string>" [--output-cols <cols>] [--show-all] [--pretty] [--result-count] [--row-count]
                      Searches project data using SQL-like syntax.
                      Results are stored for piping (e.g., to 'export').
                      Example: query run "pages.content LIKE '%error%'"

  db_info [--expand]  Shows database schema information.
                      Default: Lists logical table names.
                      --expand: Shows detailed columns for each table.

  table_info <table_name>
                      Shows the column details for a specific logical table.

  db_file [--project <id>] [--open]
                      Prints absolute path to the project's SQLite DB.
                      --open tries to open it in the OS-associated app.

  db_link [--project <id>] [--open]
                      Prints a file:// URI to the project's SQLite DB.
                      --open tries to open it in the OS-associated app.

Output options (only for 'run' subcommand):
  --output-cols <cols>  Comma-separated list of columns to select (default: *).
  --result-count        Show the count of rows matching the query.
  --row-count           Show the total count of rows in the table (ignores query).
  --show-all            Show all results instead of the first 10.
  --pretty              Show results as JSON instead of a table.
""".strip()

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "run": None,
    "db_info": None,
    "table_info": None,
    "db_file": None,
    "db_link": None,
}


def _handle_table_info(project_id: int, table_name_arg: str, ctx: ShellContext) -> int:
    service = QueryService()
    display_name, df_columns = service.get_single_table_info(project_id, table_name_arg, ctx)
    if display_name is None or df_columns is None:
        return 1
    print(f"\n--- Schema for Table '{display_name}' (Project {project_id}) ---")
    if not df_columns.empty:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.colheader_justify', 'left')
        print(df_columns.to_string(index=False, justify='left'))  # <-- Links uitgelijnd
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
        pd.reset_option('display.width')
        pd.reset_option('display.colheader_justify')
    else:
        print("   (No columns found for this table)")
    return 0


def _resolve_project_id(ctx: ShellContext, explicit: Optional[int]) -> Optional[int]:
    if explicit is not None:
        return explicit
    pid = ctx.get("project.id")
    try:
        return int(pid) if pid is not None else None
    except (TypeError, ValueError):
        return None


def _ensure_db_exists(ctx: ShellContext, project_id: int) -> Path:
    base_dir = PathUtils.get_cache_root()
    db_path = PathUtils.get_project_db_path(project_id, base_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        ctx.db_mgr.init_db_schema(project_id)
    return db_path


def _open_file(path: Path) -> bool:
    try:
        sysname = platform.system().lower()
        if sysname.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sysname == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
        return True
    except Exception as e:
        logger.warning("Failed to open file: %s", e)
        return False


def handle_query(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    parser = argparse.ArgumentParser(prog="query", description="Query project data or show DB schema.")
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    p_run = subparsers.add_parser("run", help="Execute a query string.")
    p_run.add_argument("query_string", help="The query string.")
    p_run.add_argument("--output-cols", help="Comma-separated list of columns to select (default: all).")
    p_run.add_argument("--result-count", action="store_true", help="Only show the count of matching rows.")
    p_run.add_argument("--row-count", action="store_true",
                       help="Only show the total count of rows in the table (ignores WHERE).")

    p_run.add_argument("--show-all", action="store_true", help="Show all query results.")
    p_run.add_argument("--pretty", action="store_true", help="Output query results as JSON.")

    p_db_info = subparsers.add_parser("db_info", help="Show database schema information.")
    p_db_info.add_argument("--expand", action="store_true", help="Show detailed columns for each table.")

    p_table = subparsers.add_parser("table_info", help="Show schema for a specific logical table.")
    p_table.add_argument("table_name", help="The logical name of the table.")

    p_db_file = subparsers.add_parser("db_file", help="Show the absolute path to the project's DB")
    p_db_file.add_argument("--project", type=int, help="Project ID (default: active project)")
    p_db_file.add_argument("--open", action="store_true", help="Open the DB file in associated app")

    p_db_link = subparsers.add_parser("db_link", help="Show file:// URI to the project's DB")
    p_db_link.add_argument("--project", type=int, help="Project ID (default: active project)")
    p_db_link.add_argument("--open", action="store_true", help="Open the DB file in associated app")

    try:
        if not args:
            parser.print_help()
            return 0
        parsed_args = parser.parse_args(args)
    except SystemExit:
        return 1
    except argparse.ArgumentError as e:
        print(f"Argument Error: {e}")
        return 1

    # project id
    project_id = _resolve_project_id(ctx, getattr(parsed_args, "project", None))
    if project_id is None and parsed_args.subcommand not in {"db_info", "table_info", "run"}:
        print("❌ Error: No active project and no --project specified.")
        return 1
    if project_id is None:
        pid_str = ctx.get("project.id")
        try:
            project_id = int(pid_str) if pid_str is not None else None
        except (TypeError, ValueError):
            project_id = None

    service = QueryService()

    if parsed_args.subcommand == "db_info":
        logical_schema = service.get_logical_schema_info(project_id, ctx)  # project_id may be None in service
        if logical_schema is None:
            print("   (Could not retrieve logical schema information)")
            return 1
        print(f"\n--- Logical Database Structure for Project {project_id if project_id is not None else '(n/a)'} ---")
        if not logical_schema:
            print("   (No tables found or mapped)")
            return 0
        if parsed_args.expand:
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            pd.set_option('display.colheader_justify', 'left')
            for logical_name, df_columns in logical_schema.items():
                print(f"\n📋 Logical Table: {logical_name}")
                if not df_columns.empty:
                    print(df_columns.to_string(index=False, justify='left'))  # <-- Links uitgelijnd
                else:
                    print("   (Schema details not available or table empty)")
            pd.reset_option('display.max_rows')
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
            pd.reset_option('display.colheader_justify')
        else:
            print("   Available Logical Tables:")
            for logical_name in logical_schema.keys():
                print(f"     - {logical_name}")
            print("\n   (Use 'query db_info --expand' to see columns)")
            print("   (Use 'query table_info <table_name>' for details of one table)")
        return 0

    if parsed_args.subcommand == "table_info":
        if project_id is None:
            print("❌ Error: No active project.")
            return 1
        return _handle_table_info(project_id, parsed_args.table_name, ctx)

    if parsed_args.subcommand == "run":
        if project_id is None:
            print("❌ Error: No active project.")
            return 1

        output_cols_str = getattr(parsed_args, "output_cols", None)
        output_cols_list: Optional[List[str]] = None
        if output_cols_str:
            output_cols_list = [col.strip() for col in output_cols_str.split(',') if col.strip()]

            col_regex = re.compile(r'^[a-zA-Z0-9_]+$')
            invalid_cols = [col for col in output_cols_list if not col_regex.match(col)]
            if invalid_cols:
                print(f"❌ Error: Invalid characters in column names: {', '.join(invalid_cols)}")
                print("   Hint: Gebruik alleen letters, cijfers en underscores.")
                return 1

        result: Optional[Union[pd.DataFrame, int]] = service.parse_and_execute(
            ctx=ctx,
            project_id=project_id,
            query_string=parsed_args.query_string,
            output_cols=output_cols_list,
            result_count=parsed_args.result_count,
            row_count=parsed_args.row_count
        )

        if result is None:
            return 1  # Service heeft al een error geprint

        if isinstance(result, int):
            count_type = "Total rows in table" if parsed_args.row_count else "Matching rows"
            print(f"🔍 {count_type}: {result}")
            # Sla een simpele DataFrame op voor piping
            ctx.search_result_cache = pd.DataFrame([{'count': result}])
            return 0

        result_df = result
        ctx.search_result_cache = result_df
        print(f"🔍 Query found {len(result_df)} items. Result stored for piping.")

        if result_df.empty:
            print("   (No results to display)")
        elif parsed_args.pretty:
            df_to_print = result_df if parsed_args.show_all else result_df.head(10)
            records = df_to_print.to_dict(orient='records')
            print(json.dumps(records, indent=2, ensure_ascii=False))
            if not parsed_args.show_all and len(result_df) > 10:
                print(f"\n   ... (JSON output limited to first 10 of {len(result_df)} rows)")
        elif parsed_args.show_all:
            print("\n--- All Results (Table) ---")
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            print(result_df.to_string(index=False, justify='left'))  # <-- Links uitgelijnd
            pd.reset_option('display.max_rows')
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
        else:
            print("\n--- First 10 Results (Table) ---")
            pd.set_option('display.max_rows', 15)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            print(result_df.head(10).to_string(index=False, justify='left'))  # <-- Links uitgelijnd
            pd.reset_option('display.max_rows')
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
            if len(result_df) > 10:
                print(f"   ... ({len(result_df) - 10} more rows available)")
        return 0

    if parsed_args.subcommand == "db_file":
        if project_id is None:
            print("❌ Error: No active project.")
            return 1
        db_path = _ensure_db_exists(ctx, project_id).resolve()
        print(str(db_path))
        if getattr(parsed_args, "open", False):
            if not _open_file(db_path):
                print("⚠️  Could not open DB file; see logs.")
        return 0

    if parsed_args.subcommand == "db_link":
        if project_id is None:
            print("❌ Error: No active project.")
            return 1
        db_path = _ensure_db_exists(ctx, project_id).resolve()
        try:
            uri = db_path.as_uri()
        except ValueError:
            uri = "file:///" + str(db_path).replace("\\", "/")
        print(uri)
        if getattr(parsed_args, "open", False):
            if not _open_file(db_path):
                print("⚠️  Could not open DB file; see logs.")
        return 0

    print(f"Internal error: Unknown subcommand '{parsed_args.subcommand}'.")
    parser.print_help()
    return 1