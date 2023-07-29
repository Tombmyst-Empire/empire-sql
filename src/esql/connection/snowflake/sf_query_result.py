from __future__ import annotations

from typing import Any, Generator

from empire_commons.types_ import JsonType

from esql.connection.base_query_result import BaseQueryResult


class SFQueryResult(BaseQueryResult):
    __slots__ = (
        'query_id',
    )

    def __init__(
            self,
            query_id: str | None,
            error_message: str | None,
            results: JsonType | Generator[JsonType, Any, None] | None
    ):
        super().__init__(error_message, results)
        self.query_id: str = query_id
