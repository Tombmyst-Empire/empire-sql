from __future__ import annotations

from esql.sql_.adapters.adapter_util import escape_unescaped_quotes_in_string
from esql.sql_.adapters.snowflake.snowflake_values import SnowflakeValues


def test_handle_integer():
    assert SnowflakeValues.prepare_value_by_deducing_python_type(123) == "123"

def test_handle_float():
    assert SnowflakeValues.prepare_value_by_deducing_python_type(123.456) == "123.456"

def test_handle_string_without_quotes():
    assert SnowflakeValues.prepare_value_by_deducing_python_type("hello world") == "'hello world'"

def test_handle_string_with_unescaped_single_quotes():
    # Using the earlier function to check correctness.
    original_string = "It's a 'nice' day"
    expected = f"'{escape_unescaped_quotes_in_string(original_string)}'"
    assert SnowflakeValues.prepare_value_by_deducing_python_type(original_string) == expected

def test_handle_string_with_escaped_single_quotes():
    original_string = "This \\'is\\' already escaped"
    assert SnowflakeValues.prepare_value_by_deducing_python_type(original_string) == f"'{original_string}'"

def test_handle_none():
    assert SnowflakeValues.prepare_value_by_deducing_python_type(None) == "null"

def test_handle_list():
    original_list = ["a", "b's"]
    # We're assuming the list turns into the string "['a', 'b\'s']" before escaping.
    assert SnowflakeValues.prepare_value_by_deducing_python_type(original_list) == """'["a","b\\\'s"]'"""

def test_handle_dict():
    original_dict = {"key": "value's"}
    # We're assuming the dict turns into the string "{'key': 'value's'}" before escaping.
    assert SnowflakeValues.prepare_value_by_deducing_python_type(original_dict) == """'{"key":"value\\\'s"}'"""

def test_handle_other_types():
    # For this test, we'll use a custom class. Its __str__ method will return a string.
    class TestClass:
        def __str__(self):
            return "This is a TestClass object"

    original_obj = TestClass()
    expected = f"'{escape_unescaped_quotes_in_string(str(original_obj))}'"
    assert SnowflakeValues.prepare_value_by_deducing_python_type(original_obj) == expected
