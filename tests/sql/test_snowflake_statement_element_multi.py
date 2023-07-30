from __future__ import annotations

import pytest
from empire_commons.exceptions import UnexpectedTypeException

from esql.sql_.adapters.snowflake.stmt_components.snowflake_statements import StatementElementMulti
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValueTypes


def test_basic_replacement():
    element = StatementElementMulti("Value: %0", ("%% replacement", "test", None))
    assert element.get() == "Value: \"TEST\" replacement"

def test_multiple_replacements():
    element = StatementElementMulti("Values: %0 and %1", ("%% one", "first", None), ("%% two", "second", None), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Values: 'first' one and 'second' two"

def test_falsy_value_with_fallback():
    element = StatementElementMulti("Value: %0", ("%% replacement", "", "fallback"), value_type=SnowflakeValueTypes.AS_IS)
    assert element.get() == "Value: fallback"

def test_falsy_value_without_fallback():
    element = StatementElementMulti("Value: %0", ("%% ignored", "", None))
    assert element.get() == "Value:"

def test_missing_value_placeholder():
    element = StatementElementMulti("Values: %0, %1 and %2", ("%% one", "first", None))
    assert element.get() == "Values: \"FIRST\" one, %1 and %2"

def test_extra_values_ignored():
    element = StatementElementMulti("Value: %0", ("%% one", "first", None), ("%% ignored", "extra", "fallback"), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Value: 'first' one"

def test_complex_value_identifier_replacement():
    element = StatementElementMulti("Values: %0 and %1", ("List: %%", [1, 2, 3], None), ("Dict: %%", {"a": 1}, None))
    with pytest.raises(UnexpectedTypeException):
        element.get()

def test_complex_value_replacement():
    element = StatementElementMulti("Values: %0 and %1", ("List: %%", [1, 2, 3], None), ("Dict: %%", {"a": 1}, None), value_type=SnowflakeValueTypes.VALUE)

    assert element.get() == "Values: List: '[1,2,3]' and Dict: '{\"a\":1}'"

def test_special_characters():
    element = StatementElementMulti("Escape: %0", ("chars %%", "\\t\\n\\r", None))
    assert element.get() == "Escape: chars \"\\T\\N\\R\""

def test_special_characters_value():
    element = StatementElementMulti("Escape: %0", ("chars %%", "\\t\\n\\r", None), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Escape: chars '\\t\\n\\r'"

def test_out_of_order_placeholders():
    element = StatementElementMulti("Values: %1 and %0", ("%% one", "first", None), ("%% two", "second", None))
    assert element.get() == "Values: \"SECOND\" two and \"FIRST\" one"

def test_out_of_order_placeholders_value():
    element = StatementElementMulti("Values: %1 and %0", ("%% one", "first", None), ("%% two", "second", None), value_type=SnowflakeValueTypes.VALUE)
    assert element.get() == "Values: 'second' two and 'first' one"

def test_out_of_order_placeholders_as_is():
    element = StatementElementMulti("Values: %1 and %0", ("%% one", "first", None), ("%% two", "second", None), value_type=SnowflakeValueTypes.AS_IS)
    assert element.get() == "Values: second two and first one"
