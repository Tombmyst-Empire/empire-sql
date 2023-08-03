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
    def list_organization_accounts(
            *,
            history: bool = False,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the accounts in your organization, excluding managed accounts.
        :param history: Optionally includes dropped accounts that have not yet been deleted. The output of SHOW ORGANIZATION ACCOUNTS HISTORY includes additional columns related to dropped accounts.
        :param ilike_pattern: Filters the command output by account identifier. The pattern can match the account name or the account locator. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-organization-accounts
        """
        return build_statement(
            'SHOW ORGANIZATION ACCOUNTS',
            StatementElement('HISTORY', history),
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_managed_accounts(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the managed accounts created for your account. Currently used by data providers to create reader accounts for their consumers.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-managed-accounts
        """
        return build_statement(
            'SHOW MANAGED ACCOUNTS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

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
    def list_variables(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all variables defined in the current session.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-variables
        """
        return build_statement(
            'SHOW VARIABLES',
            StatementElement('LIKE %%', ilike_pattern),
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
    def list_resource_monitors(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the resource monitors in your account for which you have access privileges.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-resource-monitors
        """
        return build_statement(
            'SHOW RESOURCE MONITORS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_databases(
            *,
            terse: bool = False,
            history: bool = False,
            ilike_pattern: str | None = None,
            starts_with: str | None = None,
            limit: int | None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the databases for which you have access privileges across your entire account, including dropped databases that are still within the Time Travel retention period and, therefore, can be undropped.

        The output returns database metadata and properties, ordered lexicographically by database name. This is important to note if you wish to filter the results using the provided filters.
        :param terse: When true, only returns the following: created_on, name, kind, database_name, schema_name
        :param history: Optionally includes dropped databases that have not yet been purged (i.e. they are still within their respective Time Travel retention periods). If multiple versions of a dropped database exist, the output displays a row for each version. The output also includes an additional dropped_on column
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive.
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param limit_filter: The optional FROM 'name_string' subclause effectively serves as a “cursor” for the results. This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:
        :return:
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'DATABASES',
            StatementElement('HISTORY', history),
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_schemas(
            *,
            terse: bool = False,
            history: bool = False,
            ilike_pattern: str | None = None,
            starts_with: str | None = None,
            limit: int | None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the schemas for which you have access privileges, including dropped schemas that are still within the Time Travel retention period and, therefore, can be undropped. The command can be used to list schemas for the current/specified database, or across your entire account.

        The output returns schema metadata and properties, ordered lexicographically by database and schema name. This is important to note if you wish to filter the results using the provided filters.
        :param terse: When true, only returns the following: created_on, name, kind, database_name, schema_name
        :param history: Includes dropped schemas that have not yet been purged (i.e. they are still within their respective Time Travel retention periods). If multiple versions of a dropped schema exist, the output displays a row for each version. The output also includes an additional dropped_on column
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive.
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param limit_filter: The optional FROM 'name_string' subclause effectively serves as a “cursor” for the results. This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:
        :return:
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'SCHEMAS',
            StatementElement('HISTORY', history),
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_shares(
            *,
            ilike_pattern: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all shares available in the system:

        - Outbound shares (to consumers) that have been created in your account (as a provider).
        - Inbound shares (from providers) that are available for your account to consume.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param limit: Filters the command output based on the string of characters that appear at the beginning of the object name. The string must be enclosed in single quotes and is case-sensitive. For example, the following return different results:
        :param limit_filter: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-shares
        """
        return build_statement(
            'SHOW SHARES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_objects(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_current_database: bool = False,
            in_database: str | None = None,
            in_current_schema: bool = False,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the tables and views for which you have access privileges. This command can be used to list the tables and views for a specified
        database or schema (or the current database/schema for the session), or your entire account.
        :param terse: When true, only returns the following: created_on, name, kind, database_name, schema_name
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_current_database:
        :param in_database:
        :param in_current_schema:
        :param in_schema:
        :param indent_level:
        :return:
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'OBJECTS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElementFirst(
                StatementElement('IN ACCOUNT', in_account),
                StatementElement('IN DATABASE', in_current_database),
                StatementElement('IN DATABASE %%', in_database),
                StatementElement('IN SCHEMA', in_current_schema),
                StatementElement('IN SCHEMA %%', in_schema)
            ),
            indent_level=indent_level
        )

    @staticmethod
    def list_tables(
            *,
            terse: bool = False,
            history: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_current_database: bool = False,
            in_database: str | None = None,
            in_current_schema: bool = False,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the schemas for which you have access privileges, including dropped schemas that are still within the Time Travel retention period and, therefore, can be undropped. The command can be used to list schemas for the current/specified database, or across your entire account.

        The output returns schema metadata and properties, ordered lexicographically by database and schema name. This is important to note if you wish to filter the results using the provided filters.
        :param terse: When true, only returns the following: created_on, name, kind, database_name, schema_name
        :param history: Includes dropped schemas that have not yet been purged (i.e. they are still within their respective Time Travel retention periods). If multiple versions of a dropped schema exist, the output displays a row for each version. The output also includes an additional dropped_on column
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_current_database:
        :param in_database:
        :param in_current_schema:
        :param in_schema:
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive.
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results. Note that the actual number of rows returned might be less than the specified limit (e.g. the number of existing objects is less than the specified limit).
        :param limit_filter: The optional FROM 'name_string' subclause effectively serves as a “cursor” for the results. This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:
        :return:
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'TABLES',
            StatementElement('HISTORY', history),
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElementFirst(
                StatementElement('IN ACCOUNT', in_account),
                StatementElement('IN DATABASE', in_current_database),
                StatementElement('IN DATABASE %%', in_database),
                StatementElement('IN SCHEMA', in_current_schema),
                StatementElement('IN SCHEMA %%', in_schema)
            ),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_columns(
            *,
            ilike_pattern: str | None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            in_table: str | None = None,
            in_view: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the columns in the tables or views for which you have access privileges. This command can be used to list the columns for a specified
        table/view/schema/database (or the current schema/database for the session), or your entire account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account: If you specify the keyword ACCOUNT, then the command retrieves records for all schemas in all databases of the current account.
        :param in_database: The command retrieves records for all schemas of the specified database.
        :param in_schema: The command retrieves records for the specified database and schema
        :param in_table: The command retrieves all records for the specified table
        :param in_view:
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-columns
        """
        return build_statement(
            'SHOW COLUMNS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElementFirst(
                StatementElement('IN ACCOUNT', in_account),
                StatementElement('IN DATABASE %%', in_database),
                StatementElement('IN SCHEMA %%', in_schema),
                StatementElement('IN TABLE %%', in_table),
                StatementElement('IN VIEW %%', in_view)
            ),
            indent_level=indent_level
        )

    @staticmethod
    def list_primary_keys(
            *,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            in_table: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists primary keys for the specified table, or for all tables in the current or specified schema, or for all tables in the current or
        specified database, or for all tables in the current account.
        :param in_account: the command retrieves records for all schemas in all databases of the current account.
        :param in_database: the command retrieves records for all schemas of the specified database.
        :param in_schema:
        :param in_table:
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-primary-keys
        """
        return build_statement(
            'SHOW PRIMARY KEYS',
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('IN TABLE %%', in_table),
            indent_level=indent_level
        )

    @staticmethod
    def list_dynamic_tables(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_current_database: bool = False,
            in_database: str | None = None,
            in_current_schema: bool = False,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None,
            indent_level: int = 0
    ):
        """
        Lists the dynamic tables for which you have access privileges. The command can be used to list dynamic tables for the current/specified
        database or schema, or across your entire account.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_current_database:
        :param in_database:
        :param in_current_schema:
        :param in_schema:
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: enables fetching the specified number of rows following the first row whose object name matches the specified string

        https://docs.snowflake.com/en/sql-reference/sql/show-dynamic-tables
        """
        return build_statement(
            'SHOW DYNAMIC TABLES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElementFirst(
                StatementElement('IN DATABASE', in_current_database),
                StatementElement('IN DATABASE %%', in_database)
            ),
            StatementElementFirst(
                StatementElement('IN SCHEMA', in_current_schema),
                StatementElement('IN SCHEMA %%', in_schema)
            ),
            StatementElement('STARTS WITH', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_external_tables(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int = 0,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the external tables for which you have access privileges. The command can be used to list external tables for the current/specified
        database or schema, or across your entire account.
        :param terse: Returns columns: created_on, name, kind, database_name, schema_name
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account:
        :param in_database:
        :param in_schema:
        :param starts_with: Filters the command output based on the string of characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-external-tables
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'EXTERNAL TABLES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_event_tables(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the event tables for which you have access privileges, including dropped tables that are still within the Time Travel retention period
        and, therefore, can be undropped. The command can be used to list event tables for the current/specified database or schema, or across your
        entire account.
        :param terse: Returns columns: created_on, name, database_name, schema_name
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account:
        :param in_database:
        :param in_schema:
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-event-tables
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'EVENT TABLES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_views(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the views, including secure views, for which you have access privileges. The command can be used to list views for the current/specified
        database or schema, or across your entire account.
        :param terse: Returns columns: created_on, name, kind, database_name, schema_name
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account:
        :param in_database:
        :param in_schema:
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-views
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'VIEWS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_materialized_views(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the materialized views that you have privileges to access.
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account:
        :param in_database:
        :param in_schema:
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-materialized-views
        """
        return build_statement(
            'SHOW MATERIALIZED VIEWS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_sequences(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the sequences for which you have access privileges. This command can be used to list the sequences for a specified schema or
        database (or the current schema/database for the session), or your entire account.
        :param ilike_pattern: Optionally filters the command output by object name. The filter uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).
        :param in_account:
        :param in_database:
        :param in_schema:
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-sequences
        """
        return build_statement(
            'SHOW SEQUENCES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_functions(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the native (i.e. system-defined/built-in) scalar functions provided by Snowflake, as well as any user-defined functions (UDFs) or external functions that have been created for your account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-functions
        """
        return build_statement(
            'SHOW FUNCTIONS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_user_functions(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all user-defined functions (UDFs) for which you have access privileges. This command can be used to list the UDFs for a specified
         database or schema (or the current database/schema for the session), or across your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-user-functions
        """
        return build_statement(
            'SHOW USER FUNCTIONS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_external_functions(
            *,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all user-defined functions (UDFs) for which you have access privileges. This command can be used to list the UDFs for a specified
         database or schema (or the current database/schema for the session), or across your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-external-functions
        """
        return build_statement(
            'SHOW EXTERNAL FUNCTIONS',
            StatementElement('LIKE %%', ilike_pattern),
            indent_level=indent_level
        )

    @staticmethod
    def list_procedures(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the stored procedures that you have privileges to access.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-procedures
        """
        return build_statement(
            'SHOW PROCEDURES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_streams(
            *,
            terse: bool,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the streams for which you have access privileges. The command can be used to list streams for the current/specified database or schema,
        or across your entire account.
        :param terse: Returns columns: created_on, name, kind, database_name, schema_name, tableOn
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-streams
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'STREAMS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_tasks(
            *,
            terse: bool,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            root_only: bool = False,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the streams for which you have access privileges. The command can be used to list streams for the current/specified database or schema,
        or across your entire account.
        :param terse: Returns columns: created_on, name, kind, database_name, schema_name, tableOn
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param root_only: Filters the command output to return only root tasks (tasks with no predecessors).
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-tasks
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'STREAMS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('ROOT ONLY', root_only),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_masking_policies(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists masking policy information, including the creation date, database and schema names, owner, and any available comments.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-masking-policies
        """
        return build_statement(
            'SHOW MASKING POLICIES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_row_access_policies(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists masking policy information, including the creation date, database and schema names, owner, and any available comments.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-masking-policies
        """
        return build_statement(
            'SHOW ROW ACCESS POLICIES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_tags(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the tag information.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-tags
        """
        return build_statement(
            'SHOW TAGS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
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

    @staticmethod
    def list_secrets(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the secrets for which you have rights to see. This command can be used to list the secrets for a specified database or schema (or the
        current database/schema for the session), or your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-secrets
        """
        return build_statement(
            'SHOW SECRETS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_stages(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the stages for which you have access privileges. This command can be used to list the stages for a specified schema or database (or
        the current schema/database for the session), or your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-secrets
        """
        return build_statement(
            'SHOW STAGES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_file_formats(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the file formats for which you have access privileges. This command can be used to list the file formats for a specified database or
        schema (or the current database/schema for the session), or your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-file-formats
        """
        return build_statement(
            'SHOW FILE FORMATS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_pipes(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the pipes for which you have access privileges. This command can be used to list the pipes for a specified database or schema (or the
        current database/schema for the session), or your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-pipes
        """
        return build_statement(
            'SHOW PIPES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_streaming_channels(
            *,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the Snowpipe Streaming channels for which you have access privileges. This command can be used to list the channels for a specified
        table, database or schema (or the current database/schema for the session), or your entire account.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-channels
        """
        return build_statement(
            'SHOW CHANNELS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            indent_level=indent_level
        )

    @staticmethod
    def list_alerts(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the alerts for which you have access privileges. The command can be used to list alerts for the current/specified database or schema,
        or across your entire account.

        :param terse: Returns columns: created_on, name, kind, database_name, schema_name, schedule, state
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-alerts
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'ALERTS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_applications(
            *,
            ilike_pattern: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the applications for which you have access privileges across your entire account in the Native Apps Framework.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-applications
        """
        return build_statement(
            'SHOW APPLICATIONS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_applications_packages(
            *,
            ilike_pattern: str | None = None,
            starts_with: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the application packages for which you have access privileges across your entire account in the Native Apps Framework.

        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param starts_with: Optionally filters the command output based on the characters that appear at the beginning of the object name. Case-sensitive
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-application-packages
        """
        return build_statement(
            'SHOW APPLICATIONS PACKAGES',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('STARTS WITH %%', starts_with, value_type=SnowflakeValueTypes.TO_STRING),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )

    @staticmethod
    def list_application_roles(
            *,
            application_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists all the application roles in a specified application.

        :param application_name: Specifies the application whose application roles you want to view.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-application-roles
        """
        return format_query(f'SHOW APPLICATION ROLES {application_name}', indent_level)

    @staticmethod
    def list_privileges_in_application(
            *,
            application_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists the privileges granted to an application.

        :param application_name: Specifies the application whose application roles you want to view.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-privileges
        """
        return format_query(f'SHOW PRIVILEGES IN APPLICATION {application_name}', indent_level)

    @staticmethod
    def list_references_in_application(
            *,
            application_name: str,
            indent_level: int = 0
    ) -> str:
        """
        Lists the references defined for an application in the manifest file and the references the consumer has associated to the application.

        :param application_name: Specifies the application whose application roles you want to view.
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-references
        """
        return format_query(f'SHOW REFERENCES IN APPLICATION {application_name}', indent_level)

    @staticmethod
    def list_release_directives_in_application(
            *,
            application_name: str,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the release directives defined for an application package in the Native Apps Framework.

        :param application_name: Specifies the application whose application roles you want to view.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-release-directives
        """
        return build_statement(
            'SHOW RELEASE DIRECTIVES',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            f'IN APPLICATION PACKAGE {application_name}',
            indent_level=indent_level
        )

    @staticmethod
    def list_versions_in_application_package(
            *,
            application_name: str,
            ilike_pattern: str | None = None,
            indent_level: int = 0
    ):
        """
        Lists the versions defined the specified application package.

        :param application_name: Specifies the application whose application roles you want to view.
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-versions
        """
        return build_statement(
            'SHOW VERSIONS',
            StatementElement('LIKE %%', ilike_pattern, value_type=SnowflakeValueTypes.TO_STRING),
            f'IN APPLICATION PACKAGE {application_name}',
            indent_level=indent_level
        )

    @staticmethod
    def list_streamlits(
            *,
            terse: bool = False,
            ilike_pattern: str | None = None,
            in_account: bool = False,
            in_database: str | None = None,
            in_schema: str | None = None,
            limit: int | None = None,
            limit_filter: str | None = None,
            indent_level: int = 0
    ) -> str:
        """
        Lists the Steamlit objects for which you have access privileges.

        :param terse: Returns columns: created_on, name, database_name, schema_name, url_id
        :param ilike_pattern: Filters the command output by object name. The filter uses case-insensitive pattern matching with support for SQL wildcard characters (% and _).
        :param in_account: Returns records for the entire account.
        :param in_database: Returns records for the current database in use or for a specified database (db_name).
        :param in_schema: Returns records for the current schema in use or a specified schema (schema_name).
        :param limit: Optionally limits the maximum number of rows returned, while also enabling “pagination” of the results.
        :param limit_filter: This enables fetching the specified number of rows following the first row whose object name matches the specified string
        :param indent_level:

        https://docs.snowflake.com/en/sql-reference/sql/show-streamlits
        """
        return build_statement(
            'SHOW',
            StatementElement('TERSE', terse),
            'STREAMLITS',
            StatementElement('LIKE %%', ilike_pattern),
            StatementElement('IN ACCOUNT', in_account),
            StatementElement('IN DATABASE %%', in_database),
            StatementElement('IN SCHEMA %%', in_schema),
            StatementElement('LIMIT %%', limit, value_type=SnowflakeValueTypes.VALUE),
            StatementElement('FROM %%', limit_filter, value_type=SnowflakeValueTypes.TO_STRING),
            indent_level=indent_level
        )
