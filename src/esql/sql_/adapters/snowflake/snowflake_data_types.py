from __future__ import annotations

from empire_commons.exceptions import UnknownValueException

from datetime import datetime, time
from enum import Enum
from typing import Any, Callable
from ejson.facades.orjson_ import loads


class SnowflakeDataTypes(Enum):
    ARRAY = 'ARRAY'
    BINARY = 'BINARY'
    BOOL = 'BOOLEAN'
    DATE = 'DATE'
    FLOAT = 'FLOAT'
    INTEGER = 'INTEGER'
    OBJECT = 'OBJECT'
    STRING = 'TEXT'
    TIME = 'TIME'
    TIMESTAMP_NO_TIMEZONE = 'TIMESTAMP_NTZ'
    TIMESTAMP_LOCAL_TIMEZONE = 'TIMESTAMP_LTZ'
    TIMESTAMP_TIMEZONE = 'TIMESTAMP_TZ'
    VARIANT = 'VARIANT'

    @staticmethod
    def parse(value: str) -> SnowflakeDataTypes:
        value = value.upper()
        type_: SnowflakeDataTypes = _TYPE_MAPPING.get(value)
        if type_ is None:
            raise ValueError(f'Unknown value {value}')

        return type_

    @staticmethod
    def from_python(_type: type) -> SnowflakeDataTypes:
        try:
            return _PYTHON_MAPPING_REVERSED[_type]
        except KeyError:
            raise UnknownValueException(_type)

    def to_python(self) -> type:
        return _PYTHON_MAPPING[self.value]

    def get_initializer(self) -> Callable[[Any], Any]:
        """
        Returns the appropriate initializer for the given Snowflake type.

        Example: type is TIMESTAMP_NO_TIMEZONE, this function will return
            ``datetime.fromisoformat()``
        """
        return _INITIALIZER_MAPPING[self.value]

    def get_emptiness_evaluation_expression(self) -> str:
        return _DEFAULT_EMPTINESS_EVALUATION_MAPPING.get(self, '{value} IS NULL')


_TYPE_MAPPING = {
    # Reference: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
    'FIXED': SnowflakeDataTypes.INTEGER,
    'NUMBER': SnowflakeDataTypes.INTEGER,
    'NUMERIC': SnowflakeDataTypes.INTEGER,
    'DECIMAL': SnowflakeDataTypes.INTEGER,
    'INT': SnowflakeDataTypes.INTEGER,
    'INTEGER': SnowflakeDataTypes.INTEGER,
    'BIGINT': SnowflakeDataTypes.INTEGER,
    'SMALLINT': SnowflakeDataTypes.INTEGER,
    'TINYINT': SnowflakeDataTypes.INTEGER,
    'BYTEINT': SnowflakeDataTypes.INTEGER,
    'REAL': SnowflakeDataTypes.FLOAT,
    'FLOAT': SnowflakeDataTypes.FLOAT,
    'FLOAT4': SnowflakeDataTypes.FLOAT,
    'FLOAT8': SnowflakeDataTypes.FLOAT,
    'DOUBLE': SnowflakeDataTypes.FLOAT,
    'DOUBLE PRECISION': SnowflakeDataTypes.FLOAT,
    'TEXT': SnowflakeDataTypes.STRING,
    'VARCHAR': SnowflakeDataTypes.STRING,
    'CHAR': SnowflakeDataTypes.STRING,
    'CHARACTER': SnowflakeDataTypes.STRING,
    'STRING': SnowflakeDataTypes.STRING,
    'BINARY': SnowflakeDataTypes.BINARY,
    'VARBINARY': SnowflakeDataTypes.BINARY,
    'BOOLEAN': SnowflakeDataTypes.BOOL,
    'DATE': SnowflakeDataTypes.DATE,
    'TIME': SnowflakeDataTypes.TIME,
    'TIMESTAMP': SnowflakeDataTypes.TIMESTAMP_NO_TIMEZONE,
    'DATETIME': SnowflakeDataTypes.TIMESTAMP_NO_TIMEZONE,
    'TIMESTAMP_NTZ': SnowflakeDataTypes.TIMESTAMP_NO_TIMEZONE,
    'TIMESTAMP_LTZ': SnowflakeDataTypes.TIMESTAMP_LOCAL_TIMEZONE,
    'TIMESTAMP_TZ': SnowflakeDataTypes.TIMESTAMP_TIMEZONE,
    'VARIANT': SnowflakeDataTypes.VARIANT,
    'ARRAY': SnowflakeDataTypes.ARRAY,
    'OBJECT': SnowflakeDataTypes.OBJECT
}

_PYTHON_MAPPING = {
    SnowflakeDataTypes.ARRAY: list,
    SnowflakeDataTypes.BINARY: bytes,
    SnowflakeDataTypes.BOOL: bool,
    SnowflakeDataTypes.DATE: datetime,
    SnowflakeDataTypes.FLOAT: float,
    SnowflakeDataTypes.INTEGER: int,
    SnowflakeDataTypes.OBJECT: dict,
    SnowflakeDataTypes.STRING: str,
    SnowflakeDataTypes.TIME: time,
    SnowflakeDataTypes.TIMESTAMP_NO_TIMEZONE: datetime,
    SnowflakeDataTypes.TIMESTAMP_LOCAL_TIMEZONE: datetime,
    SnowflakeDataTypes.TIMESTAMP_TIMEZONE: datetime,
    SnowflakeDataTypes.VARIANT: Any
}

_PYTHON_MAPPING_REVERSED: dict[type | str, SnowflakeDataTypes] = {
    bool: SnowflakeDataTypes.BOOL,
    bytes: SnowflakeDataTypes.BINARY,
    datetime: SnowflakeDataTypes.TIMESTAMP_LOCAL_TIMEZONE,
    float: SnowflakeDataTypes.FLOAT,
    int: SnowflakeDataTypes.INTEGER,
    list: SnowflakeDataTypes.ARRAY,
    dict: SnowflakeDataTypes.OBJECT,
    str: SnowflakeDataTypes.STRING,
    'bool': SnowflakeDataTypes.BOOL,
    'bytes': SnowflakeDataTypes.BINARY,
    'datetime': SnowflakeDataTypes.TIMESTAMP_LOCAL_TIMEZONE,
    'float': SnowflakeDataTypes.FLOAT,
    'int': SnowflakeDataTypes.INTEGER,
    'list': SnowflakeDataTypes.ARRAY,
    'dict': SnowflakeDataTypes.OBJECT,
    'str': SnowflakeDataTypes.STRING
}

_INITIALIZER_MAPPING = {
    SnowflakeDataTypes.ARRAY: list,
    SnowflakeDataTypes.BINARY: bytes,
    SnowflakeDataTypes.BOOL: bool,
    SnowflakeDataTypes.DATE: datetime.fromisoformat,
    SnowflakeDataTypes.FLOAT: float,
    SnowflakeDataTypes.INTEGER: int,
    SnowflakeDataTypes.OBJECT: dict,
    SnowflakeDataTypes.STRING: str,
    SnowflakeDataTypes.TIME: time.fromisoformat,
    SnowflakeDataTypes.TIMESTAMP_NO_TIMEZONE: datetime.fromisoformat,
    SnowflakeDataTypes.TIMESTAMP_LOCAL_TIMEZONE: datetime.fromisoformat,
    SnowflakeDataTypes.TIMESTAMP_TIMEZONE: datetime.fromisoformat,
    SnowflakeDataTypes.VARIANT: loads
}

_DEFAULT_EMPTINESS_EVALUATION_MAPPING = {
    SnowflakeDataTypes.ARRAY: '{value}::string IS NULL OR ARRAY_SIZE({value}) = 0',
    SnowflakeDataTypes.OBJECT: '{value}::string IS NULL OR {value} = PARSE_JSON(\'{{}}\')',
    SnowflakeDataTypes.STRING: '{value}::string IS NULL OR LENGTH({value}) = 0'
}
