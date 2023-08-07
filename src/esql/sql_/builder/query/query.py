from __future__ import annotations

from enum import Enum, auto
from typing import Any

from esql.sql_.adapters.snowflake.snowflake_query import SnowflakeQuery


class Duplicates(Enum):
    ALL = auto()
    DISTINCT = auto()


class Query:
    __slots__ = (
        '_adapter',
    )

    def __init__(self, adapter = SnowflakeQuery):
        self._adapter = adapter

    def select(
            self,
            *objects_to_select: SelectObject | Any
            *,
            duplicates: Duplicates = Duplicates.ALL,

    ) -> Query:
        return self
