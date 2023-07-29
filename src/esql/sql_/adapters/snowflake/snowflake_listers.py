from __future__ import annotations
from esql.sql_.adapters.adapter_util import format_query


class SnowflakeListers:
    @staticmethod
    def list_grants_to_current_user(indent_level: int = 0) -> str:
        return format_query('SHOW GRANTS', indent_level)

    @staticmethod
    def list_grants_on_account(indent_level: int = 0) -> str:
        return format_query('SHOW GRANTS ON ACCOUNT', indent_level)

    @staticmethod
    def list_grants_on_table(table_qualified_name: str, indent_level: int = 0) -> str:
        return format_query('SHOW GRANTS ON TABLE ')
