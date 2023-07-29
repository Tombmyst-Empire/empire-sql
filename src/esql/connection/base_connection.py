from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TypeVar, Generic, Any, Generator, Callable

from empire_commons.types_ import JsonType, JsonListType
import ereport

from esql.connection.base_query_collectors import BaseQueryCollectors

TypeConnection = TypeVar('TypeConnection')
TypeQueryResult = TypeVar('TypeQueryResult')


class BaseConnection(ABC, Generic[TypeConnection, TypeQueryResult]):
    """
    Base class for connection to a database
    """
    __slots__ = (
        'user',
        'password',
        'host',
        'database',
        '_connection'
    )

    def __init__(
            self,
            user: str,
            password: str,
            host: str,
            database: str | None
    ):
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.database: str | None = database
        self._connection: TypeConnection | None = None

    @property
    def connection(self) -> TypeConnection | None:
        """
        Returns the connection object
        """
        return self._connection

    @abstractmethod
    def connect(self) -> BaseConnection:
        """
        Connects to the database
        """
        raise NotImplementedError()

    @abstractmethod
    def commit(self):
        raise NotImplementedError()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def get_cursor(self):
        """
        Returns a cursor
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_query(
            self,
            query: str,
            *query_parameters: Any,
            query_name: str | None = None,
            collector_method: Callable[
                [Any, int], JsonType | JsonListType | Generator[JsonType, Any, None]] = BaseQueryCollectors.gather_all_records,
            batch_size: int = 1,
            verbose: bool = True
    ) -> TypeQueryResult:
        """
        Executes the provided query
        :param query: The query
        :param query_parameters: The query parameters
        :param query_name: The query name (for logging purposes)
        :param collector_method: The collector method
        :param batch_size: Batch size, passed to collector method
        :param verbose: When true, emits logs of executing query and success.
        :return: A result object, inheriting from :class:`esql.connection.base_query_result.BaseQueryResult`
        """
        raise NotImplementedError()

    async def execute_query_async(
            self,
            query: str,
            *query_parameters: Any,
            query_name: str | None = None,
            collector_method: Callable[
                [Any, int], JsonType | JsonListType | Generator[JsonType, Any, None]] = None,
            batch_size: int = 1,
            verbose: bool = True
    ) -> TypeQueryResult:
        """
        Launches a query async by making a thread out of a call to ``execute_query()`` method.

        :param query: The query
        :param query_parameters: The query parameters
        :param query_name: The query name (for logging purposes)
        :param collector_method: The collector method
        :param batch_size: Batch size, passed to collector method
        :param verbose: When true, emits logs of executing query and success.
        :return: A result object, inheriting from :class:`esql.connection.base_query_result.BaseQueryResult`
        """
        if collector_method:
            return await asyncio.to_thread(self.execute_query,
                                           query,
                                           *query_parameters,
                                           query_name=query_name,
                                           collector_method=collector_method,
                                           batch_size=batch_size,
                                           verbose=verbose)
        else:
            return await asyncio.to_thread(self.execute_query,
                                           query,
                                           *query_parameters,
                                           query_name=query_name,
                                           batch_size=batch_size,
                                           verbose=verbose)

    def close(self):
        """
        If a connection has been made to a database, closes it.
        """
        if self._connection:
            try:
                ereport.info('Closing connection ...')
                self._connection.close()
                ereport.success('Closed connection!')
            except AttributeError:
                ereport.fatal('You should re-implement the close method, '
                              'because the connection does not implement the close() method')
                raise

    def __enter__(self) -> BaseConnection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()