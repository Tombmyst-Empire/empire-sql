from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ListParametersResult:
    key: str
    value: Any
    default: Any
    level: str | None
    description: str | None
