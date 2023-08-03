from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FunctionInfo:
    created_on: str
    name: str
    schema_name: str | None
    is_builtin: bool
    is_aggregate: bool
    is_ansi: bool
    min_num_arguments: int
    max_num_arguments: int
    arguments: str
    description: str
    catalog_name: str | None
    is_table_function: bool
    valid_for_clustering: bool
    is_secure: bool
    is_external_function: bool
    language: str
    is_memoizable: bool
