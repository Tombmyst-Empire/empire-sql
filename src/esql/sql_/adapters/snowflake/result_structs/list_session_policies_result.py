from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class ListSessionPoliciesResult:
    created_on: datetime
    name: str
    database_name: str
    schema_name: str
    owner: str
    comment: str | None
    options: Any
