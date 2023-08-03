from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ListDynamicTablesResult:
    created_on: str
    name: str
    reserved: str | None
    database_name: str
    schema_name: str
    cluster_by: str
    rows: int
    bytes: int
    owner: str
    target_lag: str
    refresh_mode: str
    refresh_mode_reason: str | None
    warehouse: str
    comment: str | None
    text: str
    automatic_clustering: bool | None
    scheduling_state: str
    last_suspended_on: str | None
    is_clone: bool
    is_replica: bool
    data_timestamp: str
