from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Callable, Generator

import ereport
from empire_commons.types_ import JsonType, JsonListType
import snowflake.connector as sf
import snowflake.connector.constants as sf_constants
import snowflake.connector.cursor as sf_cursor
from snowflake.connector.network import SnowflakeRestful

from esql._internal.ref import REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME
from esql.connection.base_connection import BaseConnection
from esql.connection.snowflake.sf_query_collectors import SFQueryCollectors
from esql.connection.snowflake.sf_query_result import SFQueryResult
from esql.connection.snowflake.error_interpreter import interpret_programming_error


LOGGER = ereport.get_or_make_reporter(REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME)


class SFConnection(BaseConnection[sf, SFQueryResult]):
    __slots__ = (
        'warehouse',
        'region',
        'role'
    )

    def __init__(
            self,
            user: str,  # TODO, ramener empire.config et utiliser
            password: str,
            account: str,
            warehouse: str | None = None,
            database: str | None = None,
            region: str | None = None,
            role: str | None = None
    ):
        super().__init__(user, password, account, database)
        self.warehouse: str | None = warehouse
        self.region: str | None = region
        self.role: str | None = role

    @property
    def snowflake_api(self) -> SnowflakeRestful:
        return self._connection.rest

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    @contextmanager
    def get_cursor(self):
        logging.getLogger('snowflake.connector.cursor').setLevel(logging.ERROR)
        cursor: sf.DictCursor | None = None
        try:
            cursor = self._connection.cursor(sf.DictCursor)  # noqa
            yield cursor
        finally:
            if cursor:
                cursor.close()

    def execute_query(
            self,
            query: str,
            *query_parameters: Any,
            query_name: str | None = None,
            # TODO: set default value for collector_method to None, set its actual default value in method
            collector_method: Callable[[Any, int], JsonType | JsonListType | Generator[JsonType, Any, None]] = SFQueryCollectors.gather_all_records,
            batch_size: int = 1,
            verbose: bool = True
    ):
        if not query:
            ereport.warn('No query provided')
            return SFQueryResult(None, None, None)

        with self.get_cursor() as cursor:
            if verbose:
                if query_name:
                    LOGGER.info('Executing query: %s...', query_name)
                else:
                    LOGGER.info('Executing query: %s', query.replace('\n', ' ').replace('\t', ' ').replace('  ', ' ')[:500] if query else None)

            try:
                cursor.execute(query, query_parameters)
            except sf.errors.ProgrammingError as e:
                interpret_programming_error(query, e)
                return SFQueryResult(
                    query_id=e.sfqid,
                    error_message=e.msg,
                    results=None
                )

            if verbose:
                if query_name:
                    LOGGER.success('Successfully executed query %s', query_name)
                else:
                    LOGGER.success('Successfully executed query.')

            return SFQueryResult(
                query_id=cursor.sfqid,
                error_message=', '.join(cursor.messages) if cursor.messages else None,
                results=collector_method(cursor, batch_size)
            )

    def connect(self) -> SFConnection:
        if self._connection:
            LOGGER.debug('Re-using same connection')
            return self._connection

        LOGGER.info('Initiating connection to Snowflake ...')
        LOGGER.info('User = %s, Account = %s, Warehouse = %s, region = %s, database = %s', self.user, self.host, self.warehouse, self.region,
                     self.database)

        self._connection = sf.connect(
            user=self.user,
            password=self.password,
            account=self.host,
            warehouse=self.warehouse.value,
            database=self.database,
            region=self.region,
            role=self.role
        )

        if not self._connection:
            raise RuntimeError('Could not establish connection, _connection is None')

        ereport.success('Successfully connected.')
        return self

    def start_query(self, query: str, *parameters) -> tuple[sf_cursor, str]:
        """
        Starts *query* execution asynchronously and returns the query ID.
        """
        cursor = self._connection.cursor()
        cursor.execute_async(query, parameters)
        return cursor, cursor.sfqid

    def get_query_result_schema(self, query: str, *parameters) -> list[sf_cursor.ResultMetadata]:
        with self.get_cursor() as cursor:
            return cursor.describe(query, parameters)

    def get_query_status(self, sfqid: str) -> sf_constants.QueryStatus:
        # TODO: with async_query, monitor status (also find a way to count records while it's running)
        return self._connection.get_query_status(sfqid)

    def is_query_still_running(self, sfqid: str) -> bool:
        return self._connection.is_still_running(self._connection.get_query_status(sfqid))

    def is_query_errored(self, sfqid: str) -> bool:
        return self._connection.is_an_error(self._connection.get_query_status(sfqid))
