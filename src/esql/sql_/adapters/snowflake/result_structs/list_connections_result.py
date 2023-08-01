from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListConnectionsResult:
    """
    - region_group: Region group where the account is located. Note: This column is only displayed for organizations that span multiple region groups.
    - snowflake_region: Snowflake Region where the account is located. A Snowflake Region is a distinct location within a cloud platform region that is isolated from other Snowflake Regions. A Snowflake Region can be either multi-tenant or single-tenant (for a Virtual Private Snowflake account).
    - created_on: Date and time when the connection was created.
    - account_name: Name of the account. An organization administrator (i.e. a user with the ORGADMIN role) can change the account name.
    - name: Name of the connection.
    - comment: Comment for the connection.
    - is_primary: Indicates whether the connection is a primary connection.
    - primary: Organization name, account name, and connection name of the primary connection. This value can be copied into the AS REPLICA OF clause of the CREATE CONNECTION command when creating secondary connections.
    - failover_allowed_to_accounts: A list of any accounts that the primary connection can redirect to.
    - connection_url: Connection URL that users pass to a client to establish a connection to Snowflake.
    - organization_name: Name of your Snowflake organization.
    - account_locator: Account locator in a region.

    https://docs.snowflake.com/en/sql-reference/sql/show-connections#output
    """
    region_group: str
    snowflake_region: str
    created_on: datetime
    account_name: str
    name: str
    comment: str | None
    is_primary: bool
    primary: str
    failover_allowed_to_accounts: list
    connection_url: str
    organization_name: str
    account_locator: str
