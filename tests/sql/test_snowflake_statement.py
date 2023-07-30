from __future__ import annotations

from esql.sql_.adapters.snowflake.stmt_components.snowflake_statements import StatementElementFirst, StatementElement, StatementElementMulti


def test_snowflake_statement_application_role():
    assert StatementElementFirst(
        StatementElementMulti('APPLICATION ROLE %0%1', ('%%.', 'app_name'), ('%%', 'role_name'))
    ).get() == 'APPLICATION ROLE "APP_NAME"."ROLE_NAME"'

    assert StatementElementFirst(
        StatementElementMulti('APPLICATION ROLE %0%1', ('%%.', None), ('%%', 'role_name'))
    ).get() == 'APPLICATION ROLE "ROLE_NAME"'
