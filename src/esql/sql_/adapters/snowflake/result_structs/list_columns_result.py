from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ListColumnsResult:
    table_name: str
    schema_name: str
    column_name: str
    data_type: str
    null: bool
    default: str | None
    kind: str
    expression: str
    comment: str | None
    database_name: str
    autoincrement: str | None
