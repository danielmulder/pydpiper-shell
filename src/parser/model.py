# ============================================
# file: src/parser/model.py
# ============================================
from __future__ import annotations
from pydantic import BaseModel

class ParserSettings(BaseModel):
    parse_img: bool = False

