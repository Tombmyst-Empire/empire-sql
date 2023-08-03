from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ListExternalTablesResult:
    created_on: str
    name: str
    database_name: str
    schema_name: str
    invalid: bool
    invalid_reason: str | None
    owner: str
    comment: str | None
    stage: str
    location: str | None
    file_format_name: str
    file_format_type: str
    cloud: str
    region: str
    notification_channel: str
    last_refreshed_on: str
    table_format: str
    last_refresh_details: str | None
    owner_role_type: str | None
