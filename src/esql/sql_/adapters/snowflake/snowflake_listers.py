from __future__ import annotations

from esql._internal.ref import DEFAULT_REPORTER
from esql.sql_.adapters.adapter_util import format_query
from empire_commons.functions import coalesce

from esql.sql_.adapters.snowflake.stmt_components.snowflake_statements import build_statement, StatementElement, StatementElementFirst, StatementElementMulti
from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers
from esql.sql_.adapters.snowflake.stmt_components.snowflake_objects import SnowflakeObjects
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValueTypes

LOGGER = DEFAULT_REPORTER


class SnowflakeListers:
    @staticmethod
    def list_grants_to_current_user(indent_level: int = 0) -> str:
        """
        Lists all the roles granted to the current user.
        """
        return format_query('SHOW GRANTS', indent_level)

    @staticmethod
    def list_grants_on_account(indent_level: int = 0) -> str:
        """
        Lists all the account-level (i.e. global) privileges that have been granted to roles.
        """
        return format_query('SHOW GRANTS ON ACCOUNT', indent_level)

    @staticmethod
    def list_grants_on_object(object_type: SnowflakeObjects, qualified_name: str, indent_level: int = 0) -> str:
        """
        Lists all privileges that have been granted on the object.
        """
        return format_query(
            f'SHOW GRANTS ON {object_type.value} {SnowflakeIdentifiers.format_qualified_name(qualified_name)}',
            indent_level
        )

    @staticmethod
    def list_grants_on_application(
            application_name: str,
            application_role: str | None = None,
            role: str | None = None,
            share: str | None = None,
            in_application_package: str | None = None,
            user: str | None = None,
            indent_level: int = 0
    ):
        """
        Lists all the privileges and roles granted to the application.
        :param application_name:
        :param application_role: Lists all the privileges and roles granted to the application role. The name of the application, app_name, is optional. If not specified, Snowflake uses the current application. If the application is not a database, this command does not return results.
        :param role: Lists all privileges and roles granted to the role. If the role has a grant on a temporary object, then the grant only exists in the session that the temporary object was created.
        :param share: Lists all the privileges granted to the share.
        :param in_application_package: Lists all of the privileges and roles granted to a share in the application package.
        :param user: Lists all the roles granted to the user. Note that the PUBLIC role, which is automatically available to every user, is not listed.
        """

        if not (statement := StatementElementFirst(
            StatementElement('APPLICATION %%', application_name),
            StatementElementMulti('APPLICATION ROLE %0%1', ('%%.', application_name), ('%%', application_role)),
            StatementElement('ROLE %%', role),
            StatementElementMulti('SHARE %0 %1', ('%%', share), ('IN APPLICATION PACKAGE %%', in_application_package)),
            StatementElement('USER %%', user)
        ).get()):
            LOGGER.info('One of these parameters must be provided: "application_name", "application_role", "role", '
                        '"share", "in_application_package", "user"')
            return ''

        return format_query(
            f'SHOW GRANTS TO {statement}',
            indent_level
        )

    @staticmethod
    def list_grants_on_application_role(
            application_role_name: str,
            application_name: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the users and roles to which the application role has been granted.

        The name of the application, app_name, is optional. If not specified, Snowflake uses the current application.
        If the application is not a database, this command does not return results.
        """
        return format_query(
            f'SHOW GRANTS OF APPLICATION ROLE {f"{application_name}." if application_name else ""}{application_role_name}',
            indent_level
        )

    @staticmethod
    def list_grants_on_role(
            role_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all users and roles to which the role has been granted.
        """
        return format_query(
            f'SHOW GRANT OF ROLE {role_name}',
            indent_level
        )

    @staticmethod
    def list_grants_on_share(
            share: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the accounts for the share and indicates the accounts that are using the share.
        """
        return format_query(
            f'SHOW GRANTS OF SHARE {share}',
            indent_level
        )

    @staticmethod
    def list_future_grants_on_schema(
            schema_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all privileges on new (i.e. future) objects of a specified type in the schema granted to a role. database_name. specifies the database
        in which the schema resides and is optional when querying a schema in the current database.
        """
        return format_query(
            f'SHOW FUTURE GRANTS IN SCHEMA {SnowflakeIdentifiers.format_qualified_name(schema_name)}',
            indent_level
        )

    @staticmethod
    def list_future_grants_on_database(
            database_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all privileges on new (i.e. future) objects of a specified type in the database granted to a role.
        """
        return format_query(
            f'SHOW FUTURE GRANTS IN DATABASE {SnowflakeIdentifiers.format_identifier(database_name)}',
            indent_level
        )

    @staticmethod
    def list_future_grants_on_role(
            role_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all privileges on new (i.e. future) objects of a specified type in a database or schema granted to the role.
        """
        return format_query(
            f'SHOW FUTURE GRANTS TO ROLE {role_name}',
            indent_level
        )

    @staticmethod
    def list_integrations(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW INTEGRATIONS',
            StatementElement('%%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_api_integrations(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW API INTEGRATIONS',
            StatementElement('%%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_notification_integrations(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW NOTIFICATION INTEGRATIONS',
            StatementElement('%%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_security_integrations(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW SECURITY INTEGRATIONS',
            StatementElement('%%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_storage_integrations(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW STORAGE INTEGRATIONS',
            StatementElement('%%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_roles(
            *,
            ilike: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the roles which you can view across your entire account, including the system-defined roles and any custom roles that exist.
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW ROLES',
            StatementElement('LIKE %%', ilike, value_type=SnowflakeValueTypes.VALUE),
            indent_level=indent_level
        )

    @staticmethod
    def list_users(
            *,
            full_info: bool = True,
            ilike: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all users in the system.
        :param full_info: When false, returns: name, created_on, display_name, first_name, last_name, email, org_identity, comment, has_password, has_rsa_public_key
        :param ilike: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param limit_filter: The optional FROM 'name_string' subclause effectively serves as a “cursor” for the results. This enables fetching the specified number of rows following the first row whose object name matches the specified string. Case-sensitve. Partial names are supported.
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', not full_info),
            'USERS',
            StatementElement("LIKE %%", ilike, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement("STARTS WITH %%", starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement("FROM %%", limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )
