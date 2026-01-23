import re
import logging
from typing import List, Optional, Tuple, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class QueryCondition:
    logical_table: str
    field: str
    operator_type: str  # 'exact', 'contains', 'wildcard', 'comparison', etc.
    sql_operator: str   # '=', 'LIKE', '>', etc.
    value: str          # The value to search for
    negated: bool


@dataclass
class ParsedQuery:
    physical_table: str
    original_logical_table: str
    # List of tuples: (Connector (AND/OR/None), Condition Object)
    conditions: List[Tuple[Optional[str], QueryCondition]]


# Regex to split on AND or OR (case insensitive)
BOOLEAN_SPLIT_PATTERN = re.compile(r'\s+(AND|OR)\s+', re.IGNORECASE)

# Improved Regex: Supports =, !=, LIKE, CONTAINS, >, <, etc.
# Groups: 1=NOT, 2=Table, 3=Field, 4=Operator, 5=Value (single quote),
#         6=Value (double quote), 7=Value (numeric/word)
CONDITION_PATTERN = re.compile(
    r"(?:(NOT)\s+)?"  # Group 1: Optional NOT
    r"([\w_]+)\."     # Group 2: Table
    r"([\w_]+)\s*"    # Group 3: Field
    r"(=|!=|<>|>=|<=|>|<|LIKE|CONTAINS)\s*"  # Group 4: Operator
    r"(?:'([^']*)'|\"([^\"]*)\"|([0-9\.]+))",  # Group 5,6,7: Value types
    re.IGNORECASE
)

# --- TABLE_MAP ---
# Maps logical names (user input) to physical DB tables
TABLE_MAP: Dict[str, str] = {
    "pages": "pages",
    "internal_links": "links",
    "external_links": "links",
    "requests": "requests",
    "project": "project",
    "page_elements": "page_elements",
    "plugin_page_metrics": "plugin_page_metrics",
    "audits": "audit_issues",
    "issues": "audit_issues",
}


class QueryParseService:
    """
    Service to parse text-based queries into structured SQL-ready conditions.
    Supports boolean logic (AND/OR), table aliasing, and scope validation.
    """

    def parse(self, query_string: str) -> Optional[ParsedQuery]:
        parsed_conditions = []
        target_physical_table = None
        first_logical_table = None
        current_connector = None  # Tracks AND/OR for the next condition

        # Step 1: Split the query into parts [Condition1, AND, Condition2, OR, Condition3]
        # re.split with capture groups preserves the separators (AND/OR) in the list
        parts = BOOLEAN_SPLIT_PATTERN.split(query_string.strip())

        for i, part in enumerate(parts):
            part_strip = part.strip()
            if not part_strip:
                continue

            # If index is odd (1, 3, 5...), it is a connector (AND/OR)
            if i % 2 == 1:
                current_connector = part_strip.upper()
                continue

            # Step 2: Parse the individual condition (e.g., "pages.url LIKE '%test%'")
            match = CONDITION_PATTERN.match(part_strip)
            if not match:
                print(f"❌ Syntax Error: Invalid condition format near '{part_strip}'.")
                print("   Expected format: <table>.<field> <operator> <value>")
                return None

            # Unpack regex groups
            (not_keyword, logical_table, field, operator, val_sq, val_dq, val_num) = match.groups()

            is_negated = bool(not_keyword)
            logical_table_lower = logical_table.lower()

            # Determine the value (supports single quotes, double quotes, or numbers)
            raw_value = (
                val_sq if val_sq is not None
                else (val_dq if val_dq is not None else val_num)
            )

            # Step 3: Validate Table Scope
            current_physical_table = TABLE_MAP.get(logical_table_lower)
            if not current_physical_table:
                print(f"❌ Error: Unknown table alias '{logical_table}'.")
                print(f"   Available: {', '.join(TABLE_MAP.keys())}")
                return None

            # Constraint: All conditions in one query must target the same physical table
            if target_physical_table is None:
                target_physical_table = current_physical_table
                first_logical_table = logical_table_lower
            elif target_physical_table != current_physical_table:
                print(f"❌ Scope Error: Query mixes tables '{first_logical_table}' "
                      f"({target_physical_table}) and '{logical_table}' "
                      f"({current_physical_table}).")
                print("   Queries must be scoped to a single table context.")
                return None

            # Step 4: SQL Operator Mapping & Wildcards
            operator_upper = operator.upper()
            sql_operator = operator_upper
            operator_type = 'exact'
            final_value = raw_value

            if operator_upper == 'CONTAINS':
                sql_operator = 'LIKE'
                operator_type = 'contains'
                final_value = f"%{raw_value}%"
            elif operator_upper == 'LIKE':
                # Check if user manually added wildcards
                if '%' in raw_value:
                    operator_type = 'wildcard'
                else:
                    # Treat LIKE without % as an exact match
                    operator_type = 'exact'
            elif operator_upper in ['>', '<', '>=', '<=', '=', '!=', '<>']:
                operator_type = 'comparison'

            # Step 5: Build Condition Object
            condition = QueryCondition(
                logical_table=logical_table_lower,
                field=field,
                operator_type=operator_type,
                sql_operator=sql_operator,
                value=final_value,
                negated=is_negated
            )

            # Append with the connector found in the previous iteration
            parsed_conditions.append((current_connector, condition))

            # Reset connector for the next loop
            current_connector = None

        if not target_physical_table or not parsed_conditions:
            return None

        return ParsedQuery(
            physical_table=target_physical_table,
            original_logical_table=first_logical_table,
            conditions=parsed_conditions
        )