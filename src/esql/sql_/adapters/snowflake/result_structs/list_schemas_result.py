from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListSchemasResult:
    created_on: datetime
    name: str
    is_default: str
    is_current: str
    database_name: str
    owner: str
    comment: str | None
    options: str | None
    retention_time: int
    dropped_on: datetime | None
    owner_role_type: str | None
