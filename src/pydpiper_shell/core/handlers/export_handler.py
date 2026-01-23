# src/pydpiper_shell/core/handlers/export_handler.py
import argparse
import json
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd # Essential for reading SQL and writing Excel

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.path_utils import PathUtils
from crawler.utils.run_timers import RunTimers

# --- Import TABLE_MAP (can be shared or defined here) ---
TABLE_MAP = {
    "pages": "pages",
    "internal_links": "links",
    "external_links": "links",
    "requests": "requests",
    "project": "project",
    "page_elements": "page_elements",
    "page_metrics": "plugin_page_metrics", # Table name from page_analyser save
    # Add other mappings as needed
}
# --- End TABLE_MAP ---

logger = logging.getLogger(__name__)

# Help text remains the same
export_help_text = """
  export [<target> <id>] [-o <path>] [--table <table>]
                      Exports data to Excel. Saves to the Documents folder
                      unless an absolute path is provided with -o.
                      <target> can be a table name (e.g., pages, links, requests, page_elements, page_metrics).
                      If data is piped from 'search' or 'query', <target> and <id> are ignored.
                      Use --table explicitly if the target name is ambiguous.
""".strip()

COMMAND_HIERARCHY = None # No subcommands for completion


def handle_export(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Exports data (from DB table or piped DataFrame) to an Excel file.

    Args:
        args: Command line arguments.
        ctx: The current shell context (contains db_mgr and potentially search_result_cache).
        _stdin: Piped input (unused directly, data comes via ctx.search_result_cache).

    Returns:
        0 for success, 1 for errors.
    """
    timer = RunTimers()
    timer.start()
    df_to_export: Optional[pd.DataFrame] = None
    output_file: Optional[Path] = None
    source_description = "exported_data" # Default description for filename

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(prog="export", description="Export project data to Excel.")
    # Positional arguments for direct export (optional if piped)
    parser.add_argument("target", nargs='?', help="Data target to export (e.g., 'pages', 'links', 'page_metrics'). Ignored if data is piped.")
    parser.add_argument("project_id", type=int, nargs='?', help="Project ID to export from. Ignored if data is piped or active project used.")
    # Optional output file argument
    parser.add_argument("--output", "-o", help="Output file path (absolute or relative to Documents).")
    # Optional explicit table name
    parser.add_argument("--table", help="Explicitly specify the database table name (overrides target mapping).")

    try:
        # Use parse_args, handle potential errors if arguments are missing later
        parsed_args = parser.parse_args(args)
    except SystemExit:
        return 1 # Argparse handled help or error
    except argparse.ArgumentError as e:
        print(f"Argument Error: {e}")
        return 1

    # --- Determine Data Source ---
    is_piped = hasattr(ctx, 'search_result_cache') and isinstance(ctx.search_result_cache, pd.DataFrame) and not ctx.search_result_cache.empty
    project_id = 0 # Will be determined later if needed

    if is_piped:
        print("Piped data detected. Preparing to export DataFrame...")
        df_to_export = ctx.search_result_cache
        ctx.search_result_cache = None # Clear after consuming
        source_description = "search_result"
        # Try to get project ID from context if available (for default filename)
        project_id_str = ctx.get("project.id")
        if project_id_str:
             try: project_id = int(project_id_str)
             except ValueError: pass # Ignore if invalid
        if project_id:
             source_description = f"project_{project_id}_search_result"
        else:
             source_description = "piped_search_result" # Fallback if no active project ID

    else:
        # --- Direct Database Export ---
        target_name = parsed_args.target
        explicit_table_name = parsed_args.table
        project_id_arg = parsed_args.project_id

        # Determine Project ID
        if project_id_arg is not None:
            project_id = project_id_arg
        else:
            project_id_str = ctx.get("project.id")
            if not project_id_str:
                print("❌ Error: No project ID specified and no active project loaded.")
                print("   Usage: export <target> <project_id> [options]")
                print("   Or load a project first: project load <id>")
                return 1
            try:
                project_id = int(project_id_str)
            except (ValueError, TypeError):
                print(f"❌ Error: Invalid active project ID in context: '{project_id_str}'.")
                return 1

        # Determine Table Name
        if explicit_table_name:
            table_name = explicit_table_name
            source_description = f"project_{project_id}_{table_name}"
        elif target_name:
            table_name = TABLE_MAP.get(target_name.lower())
            if not table_name:
                 print(f"❌ Error: Unknown export target '{target_name}'.")
                 print(f"   Known targets: {', '.join(TABLE_MAP.keys())}")
                 print(f"   Or use --table <actual_table_name>.")
                 return 1
            source_description = f"project_{project_id}_{target_name}"
        else:
            print("❌ Error: Export target (e.g., 'pages', 'links') or --table is required for direct export.")
            parser.print_help()
            return 1

        # --- Load data from DB using SQL ---
        logger.info(f"Loading data from table '{table_name}' for project {project_id}...")
        try:
            conn = ctx.db_mgr.get_connection(project_id)
            sql_query = f"SELECT * FROM {table_name}"

            # Add filters based on table name (e.g., for links, project_id)
            params = []
            where_clauses = []

            # Check if table has project_id column
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [info[1] for info in cursor.fetchall()]

            if 'project_id' in columns and table_name != 'project': # Don't filter project table by itself
                 where_clauses.append("project_id = ?")
                 params.append(project_id)

            # Specific filters for combined 'links' table based on logical target
            if table_name == 'links':
                 if target_name.lower() == "internal_links":
                     where_clauses.append("is_external = 0")
                 elif target_name.lower() == "external_links":
                      where_clauses.append("is_external = 1")

            # Construct final query
            if where_clauses:
                 sql_query += " WHERE " + " AND ".join(where_clauses)

            logger.debug(f"Executing SQL: {sql_query} with params: {params}")
            df_to_export = pd.read_sql_query(sql_query, conn, params=params if params else None)

            if df_to_export.empty:
                print(f"🤷 No data found in table '{table_name}' for project {project_id}.")
                # Don't exit yet, might just be an empty table
            else:
                 logger.info(f"Loaded {len(df_to_export)} rows from '{table_name}'.")

        except pd.io.sql.DatabaseError as e:
             if "no such table" in str(e).lower():
                  print(f"❌ Error: Table '{table_name}' does not exist in the database for project {project_id}.")
             else:
                  print(f"❌ Database Error loading data: {e}")
             logger.error(f"Database error loading table '{table_name}': {e}", exc_info=True)
             return 1
        except Exception as e:
            print(f"❌ Error loading data from database: {e}")
            logger.error(f"Error loading data from table '{table_name}': {e}", exc_info=True)
            return 1

    # --- Check if DataFrame is valid ---
    if df_to_export is None:
         # This case should ideally not be reached if logic above is correct
         print("❌ Error: Failed to load data for export.")
         return 1
    if df_to_export.empty:
        print("🤷 No data available to export.")
        return 0 # Successful operation, but nothing exported

    # --- Determine Output File Path ---
    documents_dir = PathUtils.get_user_documents_dir()
    if parsed_args.output:
        user_path = Path(parsed_args.output)
        output_file = user_path if user_path.is_absolute() else documents_dir / user_path
        # Ensure suffix is .xlsx
        output_file = output_file.with_suffix(".xlsx")
    else:
        # Default filename in Documents
        output_file = documents_dir / f"{source_description}.xlsx"

    # Ensure the target directory exists
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"❌ Error creating output directory '{output_file.parent}': {e}")
        return 1

    # --- Export DataFrame to Excel ---
    try:
        logger.info(f"Exporting {len(df_to_export)} rows from '{source_description}' to {output_file}...")
        # Convert columns stored as JSON strings back to objects temporarily?
        # Or just export them as strings? Exporting as strings is safer for Excel.
        # Let's ensure complex Python objects (if any somehow remained) are stringified.
        df_export_safe = df_to_export.copy()
        for col in df_export_safe.columns:
            # Check if column type is object and contains non-scalar types
            if df_export_safe[col].dtype == 'object':
                 mask = df_export_safe[col].apply(lambda x: isinstance(x, (dict, list)))
                 if mask.any():
                      logger.debug(f"Converting complex objects in column '{col}' to strings for Excel export.")
                      # Convert only problematic cells to JSON string or repr
                      df_export_safe[col] = df_export_safe[col].apply(
                          lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                      )

        # Write to Excel using openpyxl engine
        df_export_safe.to_excel(output_file, index=False, engine='openpyxl')
        timer.stop()
        print(f"✅ Successfully exported {len(df_export_safe)} rows from '{source_description}' to:")
        print(f"   {output_file}")
        print(f"   (Completed in {timer.duration:.2f} seconds)")
        return 0
    except Exception as e:
        timer.stop()
        print(f"❌ Error during export to Excel: {e}")
        logger.error(f"Failed to export data to {output_file}: {e}", exc_info=True)
        return 1