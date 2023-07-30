from __future__ import annotations

from enum import Enum, auto
from typing import Any
from empire_commons.exceptions import UnexpectedTypeException
from ejson.facades.orjson_ import dumps

from esql.sql_.adapters.adapter_util import escape_unescaped_quotes_in_string


class SnowflakeValueTypes(Enum):
    """
    - IDENTIFIER: the value is an identifier
    - VALUE: the value is a *value* that can be used as, for example, a cell value
    - TO_STRING: the value will always be casted to string using str() and interpreted as String (enclosed within single quotes)
    - AS-IS: the value should be taken as provided, but still casted as string (not enclosed within single quotes)
    """
    IDENTIFIER = auto()
    VALUE = auto()
    VALUE_JSON = auto()
    TO_STRING = auto()
    AS_IS = auto()


class SnowflakeValues:
    @staticmethod
    def prepare_value_by_deducing_python_type(value: Any) -> str:
        if isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            return f"'{escape_unescaped_quotes_in_string(value)}'"
        elif value is None:
            return 'null'
        elif isinstance(value, (list, dict)):
            return f'\'{escape_unescaped_quotes_in_string(dumps(value))}\''
        else:
            return f"'{escape_unescaped_quotes_in_string(str(value))}'"
