from __future__ import annotations

from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListDatabasesResult:
    created_on: datetime
    name: str
    is_default: str
    is_current: str
    origin: str
    owner: str
    comment: str | None  # Optional if you expect NULL values in the table
    options: str | None  # Optional if you expect NULL values in the table
    retention_time: int
    dropped_on: datetime | None  # Optional if you expect NULL values in the tabl
