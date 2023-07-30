from __future__ import annotations
from esql.sql_.adapters.snowflake.snowflake_statements import StatementElementFirst, StatementElement, StatementElementMulti
from esql.sql_.adapters.snowflake.snowflake_values import SnowflakeValueTypes


def test_first_truthy_statement_element():
    element_1 = StatementElement("%%", None, "DefaultValue")
    element_2 = StatementElement("%%", False, "DefaultValue")
    element_3 = StatementElement("%%", "Truth", "DefaultValue")

    sef = StatementElementFirst(element_1, element_2, element_3)
    assert sef.get() == "DefaultValue"


def test_first_truthy_statement_element_no_defaults():
    element_1 = StatementElement("%%", None)
    element_2 = StatementElement("%%", False)
    element_3 = StatementElement("%%", "Truth")

    sef = StatementElementFirst(element_1, element_2, element_3)
    assert sef.get() == '"TRUTH"'


def test_no_truthy_statement_element():
    element_1 = StatementElement("%%", None, "DefaultValue")
    element_2 = StatementElement("%%", False, "DefaultValue")
    element_3 = StatementElement("%%", "", "DefaultValue")

    sef = StatementElementFirst(element_1, element_2, element_3)
    assert sef.get() == "DefaultValue"  # Assuming it returns the default value when no truthy value exists


def test_first_truthy_statement_element_multi():
    multi_1 = StatementElementMulti("%0", ("key1", None, "DefaultValue"))
    multi_2 = StatementElementMulti("%0", ("key2", "Truth", "DefaultValue"))

    sef = StatementElementFirst(multi_1, multi_2)
    assert sef.get() == "DefaultValue"


def test_mixed_elements():
    element = StatementElement("%%", "", "DefaultValue")
    multi = StatementElementMulti("%0", ("key", "Truth", "DefaultValue"))

    sef = StatementElementFirst(element, multi)
    assert sef.get() == "DefaultValue"


def test_value_type_identifier():
    element_1 = StatementElement("%%", None, "DefaultValue")
    element_2 = StatementElement("%%", "Truth", "DefaultValue")

    sef = StatementElementFirst(element_1, element_2, value_type=SnowflakeValueTypes.IDENTIFIER)
    assert sef.get() == "DefaultValue"


def test_no_statements():
    sef = StatementElementFirst()
    assert sef.get() is None  # Assuming it returns None when no input values are provided