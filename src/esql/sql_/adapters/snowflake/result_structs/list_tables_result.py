from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ListTablesInfo:
    created_on: str
    name: str
    database_name: str
    schema_name: str
    kind: str
    comment: str | None
    cluster_by: str | None
    rows: int | None
    bytes: int | None
    owner: str
    retention_time: int
    dropped_on: str | None
    automatic_clustering: str | None
    change_tracking: str | None
    search_optimization: str | None
    search_optimization_progress: str | None
    search_optimization_bytes: int | None
    is_external: bool
    owner_role_type: str | None
