from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Any, Generator

from empire_commons.types_ import JsonType, JsonListType

from esql.connection.base_connection import BaseConnection
from esql.connection.base_query_result import BaseQueryResult


class Executable(ABC):
    """
    Interface-like class that provides 2 methods to execute a SQL query.
    """
    @abstractmethod
    def execute(
            self,
            connection: BaseConnection,
            query_name: str | None = None,
            collector_method: Callable[[Any, int], JsonType | JsonListType | Generator[JsonType, Any, None]] = None,
            batch_size: int = 1,
            verbose: bool = True
    ) -> BaseQueryResult:
        """
        Executes the query
        :param connection: The connection
        :param query_name: The query name (for logging purposes)
        :param collector_method: The collector method
        :param batch_size: Batch size, passed to collector method
        :param verbose: When true, emits logs of executing query and success.
        :return: A result object, inheriting from :class:`esql.connection.base_query_result.BaseQueryResult`
        """
        raise NotImplementedError()

    @abstractmethod
    async def execute_async(
            self,
            connection: BaseConnection,
            query_name: str | None = None,
            collector_method: Callable[[Any, int], JsonType | JsonListType | Generator[JsonType, Any, None]] = None,
            batch_size: int = 1,
            verbose: bool = True
    ) -> BaseQueryResult:
        """
        Executes the query asynchronously
        :param connection: The connection
        :param query_name: The query name (for logging purposes)
        :param collector_method: The collector method
        :param batch_size: Batch size, passed to collector method
        :param verbose: When true, emits logs of executing query and success.
        :return: A result object, inheriting from :class:`esql.connection.base_query_result.BaseQueryResult`
        """
        raise NotImplementedError()
