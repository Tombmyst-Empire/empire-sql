from __future__ import annotations

import pytest
from ejson.facades.orjson_ import dumps
from empire_commons.exceptions import UnexpectedTypeException

from esql.sql_.adapters.adapter_util import escape_unescaped_quotes_in_string
from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers
from esql.sql_.adapters.snowflake.stmt_components.snowflake_statements import prepare_value
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValueTypes, SnowflakeValues


def test_identifier_string():
    identifier = "test_identifier"
    formatted = SnowflakeIdentifiers.format_qualified_name(identifier)
    assert prepare_value(identifier, SnowflakeValueTypes.IDENTIFIER) == formatted

def test_identifier_non_string():
    with pytest.raises(UnexpectedTypeException):
        prepare_value(123, SnowflakeValueTypes.IDENTIFIER)

def test_value():
    value = "test_value"
    prepared = SnowflakeValues.prepare_value_by_deducing_python_type(value)
    assert prepare_value(value, SnowflakeValueTypes.VALUE) == prepared

def test_value_json():
    original_dict = {"key": "value's"}
    json_value = escape_unescaped_quotes_in_string(dumps(original_dict))
    assert prepare_value(original_dict, SnowflakeValueTypes.VALUE_JSON) == f"PARSE_JSON('{json_value}')"

def test_to_string():
    value = 123
    assert prepare_value(value, SnowflakeValueTypes.TO_STRING) == "'123'"

def test_as_is():
    value = 123.456
    assert prepare_value(value, SnowflakeValueTypes.AS_IS) == "123.456"

def test_unexpected_value_type():
    class DummyType:
        pass

    with pytest.raises(UnexpectedTypeException):
        prepare_value("test", DummyType())

def test_value_json_with_different_types():
    # List
    original_list = ["item1", "item2"]
    json_value = escape_unescaped_quotes_in_string(dumps(original_list))
    assert prepare_value(original_list, SnowflakeValueTypes.VALUE_JSON) == f"PARSE_JSON('{json_value}')"

    # Nested structures
    nested = {"key": {"subkey": "subvalue"}}
    json_value = escape_unescaped_quotes_in_string(dumps(nested))
    assert prepare_value(nested, SnowflakeValueTypes.VALUE_JSON) == f"PARSE_JSON('{json_value}')"

def test_to_string_with_various_objects():
    # Object with a __str__ method
    class TestObj:
        def __str__(self):
            return "test_object"

    obj = TestObj()
    assert prepare_value(obj, SnowflakeValueTypes.TO_STRING) == "'test_object'"

    # None
    assert prepare_value(None, SnowflakeValueTypes.TO_STRING) == "'None'"

def test_as_is_with_various_objects():
    # Object with a __str__ method
    class TestObj:
        def __str__(self):
            return "test_object"

    obj = TestObj()
    assert prepare_value(obj, SnowflakeValueTypes.AS_IS) == "test_object"

    # None
    assert prepare_value(None, SnowflakeValueTypes.AS_IS) == "None"

    # Booleans
    assert prepare_value(True, SnowflakeValueTypes.AS_IS) == "True"
    assert prepare_value(False, SnowflakeValueTypes.AS_IS) == "False"

def test_value_special_characters():
    value = "test_value_with_special_char_'"
    prepared = f"'{escape_unescaped_quotes_in_string(value)}'"
    assert prepare_value(value, SnowflakeValueTypes.VALUE) == prepared

    # String containing escape character
    value = "test\\value"
    prepared = f"'{escape_unescaped_quotes_in_string(value)}'"
    assert prepare_value(value, SnowflakeValueTypes.VALUE) == prepared

def test_unexpected_snowflake_value_types():
    class DummySnowflakeValueTypes:
        NEW_TYPE = "new_type"

    with pytest.raises(UnexpectedTypeException):
        prepare_value("test", DummySnowflakeValueTypes.NEW_TYPE)
