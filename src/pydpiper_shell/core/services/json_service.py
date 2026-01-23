import json
from typing import Any

def to_json(data: Any, indent: int = 2, ensure_ascii: bool = False) -> str:
    """
    Convert Python object to JSON string.

    Args:
        data: Python object (dict, list, etc.)
        indent: Indentation level for pretty-printing (default: 2)
        ensure_ascii: If True, escape non-ASCII chars (default: False)

    Returns:
        str: JSON string
    """
    return json.dumps(data, ensure_ascii=ensure_ascii, indent=indent)
