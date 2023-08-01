from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class ListReplicationDatabasesResult:
    """
    https://docs.snowflake.com/en/sql-reference/sql/show-replication-databases#output

    - region_name: Region group where the account is located. Note: This column is only displayed for organizations that span multiple region groups.
    - snowflake_region: Snowflake Region where the account that stores the database is located. A Snowflake Region is a distinct location within a cloud platform region that is isolated from other Snowflake Regions. A Snowflake Region can be either multi-tenant or single-tenant (for a Virtual Private Snowflake account).
    - created_on: Date and time when the database was created.
    - account_name: Name of the account in which the database is stored.
    - name: Name of the database.
    - comment: Comment for the database.
    - is_primary: Whether the database is a primary database; otherwise, is a secondary database.
    - primary: Fully-qualified name of a primary database, including the region, account, and database name.
    - replication_allowed_to_accounts: Where IS_PRIMARY is TRUE, shows the fully-qualified names of accounts where replication has been enabled for this primary database. A secondary database can be created in each of these accounts.
    - failover_allowed_to_accounts: Where IS_PRIMARY is TRUE, shows the fully-qualified names of accounts where failover has been enabled for this primary database. A secondary database can be created in each of these accounts for business continuity and disaster recovery.
    - organization_name: Name of your Snowflake organization.
    - account_locator: Account locator in a region.
    """
    region_name: str
    snowflake_region: str
    created_on: datetime
    account_name: str
    name: str
    comment: str | None
    is_primary: bool
    primary: str
    replication_allowed_to_accounts: Any
    failover_allowed_to_accounts: Any
    organization_name: str
    account_locator: str
