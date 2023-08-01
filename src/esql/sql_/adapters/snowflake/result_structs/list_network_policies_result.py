from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListNetworkPoliciesResult:
    created_on: datetime
    name: str
    comment: str | None
    entries_in_allowed_ip_list: int
    entries_in_blocked_ip_list: int
