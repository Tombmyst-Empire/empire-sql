from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListSharesResult:
    created_on: datetime
    kind: str
    name: str
    database_name: str
    to: list[str] | None
    owner: str
    comment: str
    listing_global_name: str
