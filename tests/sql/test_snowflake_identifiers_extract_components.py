import pytest
from esql.sql_.adapters.snowflake.snowflake_identifiers import SnowflakeIdentifiers, SnowflakeIdentifierComponents


def test_extract_quoted_delimiter():
    value = '"db"."schema"."object"'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents("db", "schema", "object")

def test_extract_dot_delimiter():
    value = 'db.schema.object'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents("db", "schema", "object")

def test_extract_no_delimiter_db_flag():
    value = 'db'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents(database_name="db")

def test_extract_no_delimiter_schema_flag():
    value = 'schema'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=True)
    assert result == SnowflakeIdentifierComponents(schema_name="schema")

def test_extract_no_delimiter_object_flag():
    value = 'object'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents(object_name="object")

def test_extract_invalid_input():
    value = 'part1.part2.part3.part4'
    with pytest.raises(ValueError):
        SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=False)

def test_extract_quoted_db_schema():
    value = '"db"."schema"'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents("db", "schema", None)

def test_extract_quoted_schema_object():
    value = '"schema"."object"'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=True)
    assert result == SnowflakeIdentifierComponents(None, "schema", "object")

def test_extract_dot_db_schema():
    value = 'db.schema'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents("db", "schema", None)

def test_extract_dot_schema_object():
    value = 'schema.object'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=True)
    assert result == SnowflakeIdentifierComponents(None, "schema", "object")

def test_extract_only_db():
    value = 'db'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=True, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents(database_name="db")

def test_extract_only_schema():
    value = 'schema'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=True)
    assert result == SnowflakeIdentifierComponents(schema_name="schema")

def test_extract_only_object():
    value = 'object'
    result = SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=False)
    assert result == SnowflakeIdentifierComponents(object_name="object")

def test_extract_invalid_quoted_input():
    value = '"part1"."part2"."part3"."part4"'
    with pytest.raises(ValueError):
        SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=False)

def test_extract_invalid_dot_input():
    value = 'part1.part2.part3.part4'
    with pytest.raises(ValueError):
        SnowflakeIdentifiers.extract_components(value, starts_with_db=False, starts_with_schema=False)
