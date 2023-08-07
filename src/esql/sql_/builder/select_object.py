from __future__ import annotations

from abc import ABC
from functools import partial
from typing import Callable, Any, Type

from esql.sql_.adapters.base.stmt_components.base_values import BaseValues
from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValues


class SelectObject(ABC):
    __slots__ = (
        '_object_to_select',
    )

    def __init__(self, object_: str):
        self._object_to_select: str = object_


class SelectIdentifier(SelectObject):
    def __init__(
            self,
            name: str,
            *,
            identifier_formatter: Callable[[str | None], str] = SnowflakeIdentifiers.format_identifier
    ):
        super().__init__(identifier_formatter(name))


class SelectValue(SelectObject):
    def __init__(
            self,
            value: Any,
            *,
            value_formatter_class: Type[BaseValues] = SnowflakeValues,
            force_type: type | None = None
    ):
        if force_type is not None:
            value_formatter = partial(value_formatter_class.prepare_value_maybe_json_by_type, type_=force_type)
        else:
            value_formatter = value_formatter_class.prepare_value_maybe_json_by_deducing_python_type

        super().__init__(value_formatter(value))
