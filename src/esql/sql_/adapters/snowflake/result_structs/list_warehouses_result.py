from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from esql.sql_.adapters.snowflake.enums.warehouse_sizes import WarehouseSizes


@dataclass(frozen=True, slots=True)
class ListWarehousesResult:
    """
    - name: Name of the warehouse
    - state: Whether the warehouse is active/running (STARTED), inactive (SUSPENDED), or resizing (RESIZING).
    - type: Warehouse type; STANDARD and SNOWPARK-OPTIMIZED are the only currently supported types.
    - size: Size of the warehouse (X-Small, Small, Medium, Large, X-Large, etc.)
    - min_cluster_count: Minimum number of clusters for the (multi-cluster) warehouse (always 1 for single-cluster warehouses).
    - max_cluster_count: Maximum number of clusters for the (multi-cluster) warehouse (always 1 for single-cluster warehouses).
    - started_clusters: Number of clusters currently started.
    - running: Number of SQL statements that are being executed by the warehouse.
    - queued: Number of SQL statements that are queued for the warehouse.
    - is_default: Whether the warehouse is the default for the current user.
    - is_current: Whether the warehouse is in use for the session. Only one warehouse can be in use at a time for a session. To specify or change the warehouse for a session, use the USE WAREHOUSE command.
    - auto_suspend: Period of inactivity, in seconds, after which a running warehouse will automatically suspend and stop using credits; a null value indicates the warehouse never automatically suspends.
    - auto_resume: Whether the warehouse, if suspended, automatically resumes when a query is submitted to the warehouse.
    - available: Percentage of the warehouse compute resources that are provisioned and available.
    - provisioning: Percentage of the warehouse compute resources that are in the process of provisioning.
    - quiescing: Percentage of the warehouse compute resources that are executing SQL statements, but will be shut down once the queries complete.
    - other: Percentage of the warehouse compute resources that are in a state other than available, provisioning, or quiescing.
    - created_on: Date and time when the warehouse was created.
    - resumed_on: Date and time when the warehouse was last started or restarted.
    - updated_on: Date and time when the warehouse was last updated, which includes changing any of the properties of the warehouse or changing the state (STARTED, SUSPENDED, RESIZING) of the warehouse.
    - owner: Role that owns the warehouse.
    - comment: Comment for the warehouse.
    - enable_query_acceleration: Whether the query acceleration service is enabled for the warehouse.
    - query_acceleration_max_scale_factor: Maximum scale factor for the query acceleration service.
    - resource_monitor: ID of resource monitor explicitly assigned to the warehouse; controls the monthly credit usage for the warehouse.
    - scaling_policy: Policy that determines when additional clusters (in a multi-cluster warehouse) are automatically started and shut down.

    https://docs.snowflake.com/en/sql-reference/sql/show-warehouses#output
    """
    name: str
    state: Literal['STARTED'] | Literal['SUSPENDED'] | Literal['RESIZING']
    type: Literal['STANDARD'] | Literal['SNOWPARK-OPTIMIZED']
    size: WarehouseSizes
    min_cluster_count: int
    max_cluster_count: int
    started_clusters: int
    running: int
    queued: int
    is_default: bool
    is_current: bool
    auto_suspend: int
    auto_resume: bool
    available: float
    provisioning: float
    quiescing: float
    other: float
    created_on: datetime
    resumed_on: datetime
    updated_on: datetime
    owner: str
    comment: str | None
    enable_query_acceleration: bool
    query_acceleation_max_scale_factor: int
    resource_monitor: str | None
    uuid: str
    scaling_policy: Literal['Standard'] | Literal['Economy']
