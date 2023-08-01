from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListUsersResult:
    name: str
    created_on: datetime
    login_name: str
    display_name: str
    first_name: str | None
    last_name: str | None
    email: str | None
    mins_to_unlock: str | None
    days_to_expiry: str | None
    comment: str | None
    disabled: bool
    must_change_password: bool
    snowflake_lock: bool
    default_warehouse: str | None
    default_namespace: str | None
    default_role: str | None
    default_secondary_roles: str | None
    ext_authn_duo: bool
    ext_authn_uid: bool
    mins_to_bypass_mfa: bool
    owner: str
    last_success_login: datetime
    expires_at_time: datetime | None
    locked_until_time: datetime | None
    has_password: bool
    has_rsa_public_key: bool
