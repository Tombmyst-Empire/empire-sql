from __future__ import annotations

from typing import Any

from empire_commons import list_util


class BaseQueryResult:
    """
    Base class for query results.
    """
    __slots__ = (
        'error',
        'results'
    )

    def __init__(
            self,
            error: str | None,
            results: Any | None
    ):
        self.error: str | None = error
        self.results: Any | None = results

    @property
    def is_errored(self) -> bool:
        """
        Returns true when the query failed
        """
        return self.error is not None

    @classmethod
    def merge(cls, *others: BaseQueryResult) -> BaseQueryResult:
        """
        Merges this result with other result(s)
        """
        return BaseQueryResult(
            ', '.join(filter(None, [result.error for result in others])),
            list_util.merge_lists(*filter(None, [result.results for result in others]))
        )

    def __repr__(self) -> str:
        return f'''BaseQueryResult(
    error={self.error},
    results={self.results}
)'''
