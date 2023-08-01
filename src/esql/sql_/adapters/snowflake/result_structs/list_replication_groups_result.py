from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class ListReplicationGroupsResult:
    """
    - region_group: Region group where the account is located. Note: this column is only visible to organizations that span multiple Region Groups.
    - snowflake_region: Snowflake Region where the account is located. A Snowflake Region is a distinct location within a cloud platform region that is isolated from other Snowflake Regions. A Snowflake Region can be either multi-tenant or single-tenant (for a Virtual Private Snowflake account).
    - created_on: Date and time replication or failover group was created.
    - account_name: Name of the account.
    - name: Name of the replication or failover group.
    - type: Type of group. Valid values are REPLICATION or FAILOVER.
    - comment: Comment string
    - is_primary: Indicates whether the replication or failover group is the primary group.
    - primary: Name of the primary group.
    - object_types: List of specified object types enabled for replication (and failover in the case of a FAILOVER group).
    - allowed_integration_types: A list of integration types that are enabled for replication. Snowflake always includes this column in the output even if integrations were not specified in the CREATE <object> or ALTER <object> command.
    - allowed_accounts: List of accounts enabled for replication and failover.
    - organization_name: Name of your Snowflake organization.
    - account_locator: Account locator in a region.
    - replication_schedule: Scheduled interval for refresh; NULL if no replication schedule is set.
    - secondary_state: Current state of scheduled refresh. Valid values are started or suspended. NULL if no replication schedule is set.
    - next_scheduled_refresh: Date and time of the next scheduled refresh.
    - owner: Name of the role with the OWNERSHIP privilege on the replication or failover group. NULL if the replication or failover group is in a different region.
    """
    region_group: str
    snowflake_region: str
    created_on: datetime
    account_name: str
    name: str
    type: Literal['REPLICATION'] | Literal['FAILOVER']
    comment: str | None
    is_primary: bool
    primary: str
    object_types: list
    allowed_integration_types: list
    allowed_accounts: list
    organization_name: str
    account_locator: str
    replication_schedule: Any
    secondary_state: Literal['started'] | Literal['suspended'] | None
    next_scheduled_refresh: datetime
    owner: str | None
