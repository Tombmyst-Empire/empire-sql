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
        :param indent_level:
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
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        """
        return build_statement(
            'SHOW INTEGRATIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_api_integrations(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        """
        return build_statement(
            'SHOW API INTEGRATIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_notification_integrations(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        """
        return build_statement(
            'SHOW NOTIFICATION INTEGRATIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_security_integrations(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW SECURITY INTEGRATIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_storage_integrations(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the integrations in your account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        """
        return build_statement(
            'SHOW STORAGE INTEGRATIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_network_policies(*, indent_level: int = 0) -> str:
        """
        Lists all network policies defined in the system.
        :param indent_level:
        :return:
        """
        return format_query('SHOW NETWORK POLICIES', indent_level)

    @staticmethod
    def list_password_policies(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists password policy information, including the creation date, database and schema names, owner, and any available comments.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:
        """
        return build_statement(
            'SHOW PASSWORD POLICIES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElementFirst(
                StatementElement('IN ACCOUNT', in_account),
                StatementElement('IN DATABASE %%', in_database),
                StatementElement('IN SCHEMA %%', in_schema)
            ),
            indent_level=indent_level
        )

    @staticmethod
    def list_session_policies(
            *,
            indent_level: int = 0
    ) -> str:
        """
        Lists session policy information, including the creation date, database and schema names, owner, and any available comments.
        :param indent_level:
        """
        return format_query('SHOW SESSION POLCIIES', indent_level)

    @staticmethod
    def list_replication_accounts(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the accounts in your organization that are enabled for replication and indicates the region in which each account is located.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        """
        return build_statement(
            'SHOW REPLICATION ACCOUNTS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_replication_databases(
            *,
            ilike_pattern: str | None = None,
            primary: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the primary and secondary databases (i.e. all databases for which replication has been enabled) in your organization and indicates the region in which each account is located.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param primary: Specifies the scope of the command, which determines whether the command lists records only for the specified primary database. The account_identifier can be in the form org_name.account_name or snowflake_region.account_locator.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-replication-databases
        """
        return build_statement(
            'SHOW REPLICATION DATABASES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('WITH PRIMARY %%', primary, value_type=SnowflakeValueTypes.IDENTIFIER),
            indent_level=indent_level
        )

    @staticmethod
    def list_regions(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the regions in which accounts can be created. This command returns the Snowflake Region name, the cloud provider (AWS, Google Cloud Platform, or Microsoft Azure) that hosts the account, and the cloud provider’s name for the region.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-regions
        """
        return build_statement(
            'SHOW REGIONS',
            StatementElement('LIKE %%', ilike_pattern),
            indent_level=indent_level
        )

    @staticmethod
    def list_replication_groups(
            *,
            account: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists each primary or secondary replication or failover group in this account.
        Lists primary replication and failover groups in other accounts enabled for replication to this account.
        Lists secondary replication and failover groups in other accounts linked to groups in this account.

        :param account: Specifies the identifier for the account.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-replication-groups
        """
        return build_statement(
            'SHOW REPLICATION GROUPS',
            StatementElement('IN ACCOUNT %%', account),
            indent_level=indent_level
        )

    @staticmethod
    def list_databases_in_replication_group(
            *,
            group_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists databases in a replication group.
        :param group_name: Specifies the identifier for the replication group.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-databases-in-replication-group
        """
        return format_query(f'SHOW DATABASES IN REPLICATION GROUP {SnowflakeIdentifiers.format_identifier(group_name)}', indent_level)

    @staticmethod
    def list_shares_in_replication_group(
            *,
            group_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists shares in a replication group.
        :param group_name: Specifies the identifier for the replication group.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-shares-in-replication-group
        """
        return format_query(f'SHOW SHARES IN REPLICATION GROUP {SnowflakeIdentifiers.format_identifier(group_name)}', indent_level)

    @staticmethod
    def list_failover_groups(
            *,
            in_account: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the primary and secondary failover groups in your account, as well as the failover groups in other accounts that are associated with your account.
        :param in_account: Specifies the identifier for the account. Account name is a unique identifier within your organization.
        :param indent_level:
        https://docs.snowflake.com/en/sql-reference/sql/show-failover-groups
        """
        return build_statement(
            'SHOW FAILOVER GROUPS',
            StatementElement('IN ACCOUNT %%', in_account),
            indent_level=indent_level
        )

    @staticmethod
    def list_databases_in_failover_group(
            *,
            group_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists databases in a failover group.
        :param group_name: Specifies the identifier for the failover group.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-databases-in-failover-group
        """
        return format_query(f'SHOW DATABASES IN FAILOVER GROUP {SnowflakeIdentifiers.format_identifier(group_name)}', indent_level)

    @staticmethod
    def list_shares_in_failover_group(
            *,
            group_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists shares in a failover group.
        :param group_name: Specifies the identifier for the failover group.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-shares-in-failover-group
        """
        return format_query(f'SHOW SHARES IN FAILOVER GROUP {SnowflakeIdentifiers.format_identifier(group_name)}', indent_level)

    @staticmethod
    def list_connections(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the connections for which you have access privileges.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        :return:
        """
        return build_statement(
            'SHOW CONNECTIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_roles(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the roles which you can view across your entire account, including the system-defined roles and any custom roles that exist.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:
        """
        return build_statement(
            'SHOW ROLES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_parameters(
            *,
            ilike_pattern: str | None = None,
            in_session: bool = False,
            in_account: bool = False,
            for_current_user: bool = False,
            for_user: str | None = None,
            for_current_warehouse: bool = False,
            in_warehouse: str | None = None,
            for_current_database: bool = False,
            in_database: str | None = None,
            for_current_schema: bool = False,
            in_schema: str | None = None,
            for_current_task: bool = False,
            in_task: str | None = None,
            in_table: str | None = None,
            indent_level: int = 0
    ) -> str:
        return build_statement(
            'SHOW PARAMETERS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElementFirst(
                StatementElement('IN SESSION', in_session),
                StatementElement('IN ACCOUNT', in_account),
                StatementElement('FOR USER', for_current_user),
                StatementElement('FOR USER %%', for_user),
                StatementElement('IN WAREHOUSE', for_current_warehouse),
                StatementElement('IN WAREHOUSE %%', in_warehouse),
                StatementElement('IN DATABASE', for_current_database),
                StatementElement('IN DATABASE %%', in_database),
                StatementElement('IN SCHEMA', for_current_schema),
                StatementElement('IN SCHEMA %%', in_schema),
                StatementElement('IN TASK', for_current_task),
                StatementElement('IN TASK %%', in_task),
                StatementElement('IN TABLE %%', in_table)
            ),
            indent_level=indent_level
        )

    @staticmethod
    def list_locks(
            *,
            in_account: bool = False,
            indent_level: int = 0
    ) -> str:
        """
        Lists all running transactions that have locks on resources. The command can be used to show locks for the current user in all the user’s sessions or all users in the account.
        :param in_account: Returns all locks across all users in the account. This parameter only applies when executed by users using the ACCOUNTADMIN role (i.e. account administrators).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-locks
        """
        return build_statement(
            'SHOW LOCKS',
            StatementElement('IN ACCOUNT', in_account),
            indent_level=indent_level
        )

    @staticmethod
    def list_transactions(
            *,
            in_account: bool = False,
            indent_level: int = 0
    ) -> str:
        """
        List all running transactions. The command can be used to show transactions for the current user or all users in the account.
        :param in_account: Shows all transactions across all users in the account. It can only be used by users with the ACCOUNTADMIN role (i.e. account administrators).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-transactions
        """
        return build_statement(
            'SHOW TRANSACTIONS',
            StatementElement('IN ACCOUNT', in_account),
            indent_level=indent_level
        )

    @staticmethod
    def list_warehouses(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the warehouses in your account for which you have access privileges.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-warehouses
        """
        return build_statement(
            'SHOW WAREHOUSES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_users(
            *,
            full_info: bool = True,
            ilike_pattern: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all users in the system.
        :param full_info: When false, returns: name, created_on, display_name, first_name, last_name, email, org_identity, comment, has_password, has_rsa_public_key
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param limit_filter: The optional FROM 'name_string' subclause effectively serves as a “cursor” for the results. This enables fetching the specified number of rows following the first row whose object name matches the specified string. Case-sensitve. Partial names are supported.
        :param indent_level:
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', not full_info),
            'USERS',
            StatementElement("LIKE %%", ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement("STARTS WITH %%", starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement("FROM %%", limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )
