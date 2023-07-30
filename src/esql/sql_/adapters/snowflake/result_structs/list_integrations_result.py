from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListIntegrationsResult:
    """
    - name: Name of the integration
    - type: Type of the integration
    - category: Category of the integration
    - enabled: Current status of the integration, either TRUE (enabled) or FALSE (disabled)
    - comment: Comment for the integration
    - created_on: Date and time when the integration was created

    https://docs.snowflake.com/en/sql-reference/sql/show-integrations#output
    """
    name: str
    type: str
    category: str
    enabled: bool
    comment: str | None
    created_on: datetime
