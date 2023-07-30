from __future__ import annotations


from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers


def test_format_none_identifier():
    assert SnowflakeIdentifiers.format_identifier(None) == None

def test_format_empty_identifier():
    assert SnowflakeIdentifiers.format_identifier("") == ""

def test_format_quoted_identifier():
    assert SnowflakeIdentifiers.format_identifier('"example"') == '"example"'

def test_format_embedded_quotes_identifier():
    assert SnowflakeIdentifiers.format_identifier('exa"mple') == '"EXA""MPLE"'
    assert SnowflakeIdentifiers.format_identifier('"exa"mple"') == '"exa"mple"'
    assert SnowflakeIdentifiers.format_identifier('exa""mple') == '"EXA""""MPLE"'

def test_format_no_quotes_identifier():
    assert SnowflakeIdentifiers.format_identifier('example') == '"EXAMPLE"'

def test_format_starts_ends_embedded_quotes_identifier():
    assert SnowflakeIdentifiers.format_identifier('"exa"mple"') == '"exa"mple"'
    assert SnowflakeIdentifiers.format_identifier('"exa""mple"') == '"exa""mple"'
