# pydpiper_shell/core/services/query_service.py
import logging
from typing import Optional, Tuple, List, Dict, Any, Union

import pandas as pd

from pydpiper_shell.core.context.shell_context import ShellContext
from .query_parse_service import QueryParseService, ParsedQuery, TABLE_MAP

logger = logging.getLogger(__name__)


class QueryService:
    """
    Executes parsed queries against the database and provides logical schema information.
    Handles SQL generation based on the Parser's output.
    """

    def parse_and_execute(
            self,
            ctx: ShellContext,
            project_id: int,
            query_string: str,
            output_cols: Optional[List[str]] = None,
            result_count: bool = False,
            row_count: bool = False
    ) -> Optional[Union[pd.DataFrame, int]]:
        """
        Parses the query string using QueryParseService and executes the resulting SQL.

        Args:
            ctx: The shell context containing database managers.
            project_id: The ID of the current project.
            query_string: The raw query string input by the user.
            output_cols: Specific columns to select (optional).
            result_count: If True, returns the count of matching rows instead of data.
            row_count: If True, returns the total count of rows in the table (ignores filters).

        Returns:
            A DataFrame containing results, an integer count, or None if parsing failed.
        """
        # 1. Parse the query string
        parser = QueryParseService()
        parsed_query: Optional[ParsedQuery] = parser.parse(query_string)

        if parsed_query is None:
            return None  # Parser has already printed the error details

        table_name = parsed_query.physical_table
        conn = ctx.db_mgr.get_connection(project_id)

        try:
            # 2. Priority 1: Simple Row Count (Total rows in table)
            if row_count:
                sql_query = f'SELECT COUNT(*) FROM "{table_name}"'
                logger.info(f"Executing SQL query (row-count) for project {project_id}...")

                cursor = conn.execute(sql_query)
                count = cursor.fetchone()[0]

                logger.info(f"Query (row-count) complete. {count} total rows found.")
                return count

            # 3. Build the WHERE clause based on parsed conditions
            where_conditions: List[str] = []
            sql_params: List[Any] = []

            for connector, condition in parsed_query.conditions:
                # Use the raw value from the condition
                op = condition.sql_operator.upper()
                val = condition.value

                # Smart handling: String comparisons (LIKE, =) are made case-insensitive using LOWER().
                # Numeric comparisons (>, <, >=, <=) are left intact.
                is_numeric_op = op in ['>', '<', '>=', '<=']

                # Check if value looks like a number (handles simple decimals)
                is_numeric_val = str(val).replace('.', '', 1).isdigit()

                if not is_numeric_op and not is_numeric_val:
                    # Text comparison -> Case Insensitive
                    condition_str = f'LOWER("{condition.field}") {op} ?'
                    param = str(val).lower()
                else:
                    # Numeric comparison -> Direct
                    condition_str = f'"{condition.field}" {op} ?'
                    param = val

                # Apply Negation (NOT)
                if condition.negated:
                    condition_str = f"NOT ({condition_str})"

                # Add Connector (AND / OR), except for the very first condition
                if connector:
                    where_conditions.append(connector)

                where_conditions.append(condition_str)
                sql_params.append(param)

            full_where_clause = " ".join(where_conditions)
            if not full_where_clause:
                full_where_clause = "1=1"  # Fallback for no conditions

            # 4. Priority 2: Result Count (Rows matching filters)
            if result_count:
                sql_query = f'SELECT COUNT(*) FROM "{table_name}" WHERE {full_where_clause}'
                logger.info(f"Executing SQL query (result-count) for project {project_id}...")
                logger.debug(f"SQL: {sql_query} | Params: {sql_params}")

                cursor = conn.execute(sql_query, sql_params)
                count = cursor.fetchone()[0]

                logger.info(f"Query (result-count) complete. {count} matching rows found.")
                return count

            # 5. Priority 3: Data Retrieval
            select_clause = "*"
            if output_cols:
                # Quote column names for safety
                safe_cols = [f'"{col}"' for col in output_cols]
                select_clause = ", ".join(safe_cols)

            sql_query = f"SELECT {select_clause} FROM {table_name} WHERE {full_where_clause}"

            logger.info(f"Executing SQL query for project {project_id}...")
            logger.debug(f"SQL: {sql_query}")
            logger.debug(f"Params: {sql_params}")

            result_df = pd.read_sql_query(sql_query, conn, params=sql_params)
            logger.info(f"Query complete. {len(result_df)} rows found.")

            if not result_df.empty:
                # Metadata used for caching mechanisms downstream
                result_df._metadata = {'cache_name': parsed_query.original_logical_table}

            return result_df

        except Exception as e:
            logger.error(
                f"Error during SQL query execution for project {project_id}: {e}",
                exc_info=True
            )
            print(f"❌ Error executing query: {e}")

            # Provide user-friendly hints for common SQL errors
            err_msg = str(e).lower()
            if "no such column" in err_msg:
                offending_field = str(e).split(':')[-1].strip().replace('"', '')
                print(f"   Hint: Column '{offending_field}' does not exist in '{parsed_query.original_logical_table}'.")
                print(f"   Use 'query table_info {parsed_query.original_logical_table}' for an overview.")
            elif "no such table" in err_msg:
                print(f"   Hint: Table '{table_name}' might not exist yet.")

            return None

    def get_logical_schema_info(
            self,
            project_id: int,
            ctx: ShellContext
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Retrieves the logical schema info for all mapped tables.

        Returns:
            A dictionary mapping logical table names to DataFrames containing schema info.
        """
        logical_schema: Dict[str, pd.DataFrame] = {}
        physical_schema = ctx.db_mgr.get_schema_info(project_id)

        if not physical_schema:
            return None

        for logical_name in sorted(TABLE_MAP.keys()):
            physical_name = TABLE_MAP[logical_name]

            if physical_name in physical_schema:
                df_columns = physical_schema[physical_name].copy()

                # Filter out internal columns if necessary
                if logical_name in ["internal_links", "external_links"]:
                    df_columns = df_columns[df_columns['name'] != 'is_external'].reset_index(drop=True)

                logical_schema[logical_name] = df_columns
            else:
                logger.warning(
                    f"Physical table '{physical_name}' (for logical '{logical_name}') not found."
                )
        return logical_schema

    def get_single_table_info(
            self,
            project_id: int,
            table_name_arg: str,
            ctx: ShellContext
    ) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
        """
        Retrieves schema info for a single logical or physical table.

        Args:
            project_id: The ID of the current project.
            table_name_arg: The table name requested by the user.
            ctx: The shell context.

        Returns:
            A tuple containing (display_name, schema_dataframe).
        """
        logical_name_requested = table_name_arg.lower()

        # Try logical mapping first, otherwise treat input as physical name
        physical_name_to_find = TABLE_MAP.get(logical_name_requested, logical_name_requested)

        physical_schema = ctx.db_mgr.get_schema_info(project_id)
        if not physical_schema:
            return None, None

        if physical_name_to_find in physical_schema:
            df_columns = physical_schema[physical_name_to_find].copy()

            display_name = (
                logical_name_requested
                if logical_name_requested in TABLE_MAP
                else physical_name_to_find
            )

            # Filter specific internal columns for link tables
            if display_name in ["internal_links", "external_links"]:
                df_columns = df_columns[df_columns['name'] != 'is_external'].reset_index(drop=True)

            return display_name, df_columns
        else:
            print(f"❌ Error: Table '{table_name_arg}' (resolved: '{physical_name_to_find}') not found.")
            print(f"   Available logical tables: {', '.join(sorted(TABLE_MAP.keys()))}")
            return None, None