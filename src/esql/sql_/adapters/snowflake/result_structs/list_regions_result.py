from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ListRegionsResult:
    """
    - region_group: Region group where the account is located. Note: This column is only displayed for organizations that span multiple region groups.
    - snowflake_region: Snowflake Region where the account is located. A Snowflake Region is a distinct location within a cloud platform region that is isolated from other Snowflake Regions. A Snowflake Region can be either multi-tenant or single-tenant (for a Virtual Private Snowflake account).
    - cloud: Name of the cloud provider that hosts the account.
    - region: Region where the account is located; i.e. the cloud provider’s name for the region.
    - display_name: Human-readable cloud region name, e.g. US West (Oregon)

    https://docs.snowflake.com/en/sql-reference/sql/show-regions#output
    """
    region_group: str
    snowflake_region: str
    cloud: str
    region: str
    display_name: str
