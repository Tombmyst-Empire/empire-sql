from __future__ import annotations

import pytest
from empire_commons.exceptions import UnexpectedTypeException

from esql.sql_.adapters.snowflake.snowflake_statements import StatementElement
from esql.sql_.adapters.snowflake.snowflake_values import SnowflakeValueTypes


def test_truthy_value_with_placeholder():
    element = StatementElement("This is %%", "replaced")
    assert element.get() == "This is \"REPLACED\""


def test_truthy_value_without_placeholder():
    element = StatementElement("This is without placeholder", "replaced")
    assert element.get() == "This is without placeholder"


def test_falsy_value_with_alt_element():
    element = StatementElement("This is %%", "", "Alternative Element")
    assert element.get() == "Alternative Element"


def test_falsy_value_without_alt_element():
    element = StatementElement("This is %%", "")
    assert element.get() == None


def test_truthy_object_with_placeholder():
    class Sample:
        def __str__(self):
            return "Sample Object"

    element = StatementElement("This is %%", Sample())
    with pytest.raises(UnexpectedTypeException):
        assert element.get()


def test_truthy_object_with_placeholder_value():
    class Sample:
        def __str__(self):
            return "Sample Object"

    element = StatementElement("This is %%", Sample(), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "This is 'Sample Object'"


def test_truthy_object_with_placeholder_to_string():
    class Sample:
        def __str__(self):
            return "Sample Object"

    element = StatementElement("This is %%", Sample(), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "This is 'Sample Object'"


def test_truthy_object_with_placeholder_as_is():
    class Sample:
        def __str__(self):
            return "Sample Object"

    element = StatementElement("This is %%", Sample(), value_type=SnowflakeValueTypes.AS_IS)
    assert element.get() == "This is Sample Object"


def test_multiple_placeholders():
    element = StatementElement("%% and %%", "first")
    assert element.get() == "\"FIRST\" and %%"

def test_falsy_value_false():
    element = StatementElement("%% replacement", False, "Alternative")
    assert element.get() == "Alternative"

def test_falsy_value_none():
    element = StatementElement("%% replacement", None, "Alternative")
    assert element.get() == "Alternative"

def test_falsy_value_zero():
    element = StatementElement("%% replacement", 0, "Alternative")
    assert element.get() == "Alternative"

def test_falsy_value_empty_list():
    element = StatementElement("%% replacement", [], "Alternative")
    assert element.get() == "Alternative"

def test_falsy_value_empty_dict():
    element = StatementElement("%% replacement", {}, "Alternative")
    assert element.get() == "Alternative"

def test_numeric_element_string_and_value_identifier():
    element = StatementElement("Value is %%", 123)
    with pytest.raises(UnexpectedTypeException):
        assert element.get() == "Value is 123"

def test_numeric_element_string_and_value_value():
    element = StatementElement("Value is %%", 123, value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Value is 123"

def test_numeric_element_string_and_value_to_string():
    element = StatementElement("Value is %%", 123, value_type=SnowflakeValueTypes.TO_STRING)
    assert element.get() == "Value is '123'"

def test_numeric_element_string_and_value_as_is():
    element = StatementElement("Value is %%", 123, value_type=SnowflakeValueTypes.AS_IS)
    assert element.get() == "Value is 123"

def test_complex_number_as_value():
    element = StatementElement("Value is %%", complex(1, 2), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Value is '(1+2j)'"

def test_list_as_value():
    element = StatementElement("List is %%", [1, 2, 3], value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "List is '[1,2,3]'"

def test_list_as_value_json():
    element = StatementElement("List is %%", [1, 2, 3], value_type=SnowflakeValueTypes.VALUE_JSON)
    assert element.get() == "List is PARSE_JSON('[1,2,3]')"

def test_dict_as_value():
    element = StatementElement("Dict is %%", {"a": 1}, value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Dict is '{\"a\":1}'"

def test_dict_as_value_json():
    element = StatementElement("Dict is %%", {"a": 1}, value_type=SnowflakeValueTypes.VALUE_JSON)
    assert element.get() == "Dict is PARSE_JSON('{\"a\":1}')"

def test_special_characters_in_element_string():
    element = StatementElement("Special chars: %%", "\\t\\n\\r", value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Special chars: '\\t\\n\\r'"

