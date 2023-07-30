from __future__ import annotations


import pytest

from esql.exceptions import BadIdentifierException
from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers


def test_validate_valid_identifier():
    identifier = "A_valid_identifier"
    assert SnowflakeIdentifiers.validate_single_identifier(identifier) == True

def test_validate_too_long_identifier():
    identifier = "A" * 256
    with pytest.raises(BadIdentifierException) as excinfo:
        SnowflakeIdentifiers.validate_single_identifier(identifier)
    assert str(excinfo.value) == f'Bad identifier: Identifier is too long: 256 / 255. Identifier = {identifier}'

def test_validate_none_identifier():
    # This will currently pass as True. If this is not desired, you'll need to update the function logic.
    assert SnowflakeIdentifiers.validate_single_identifier(None) == False

def test_validate_empty_identifier():
    # This will currently pass as True. If this is not desired, you'll need to update the function logic.
    assert SnowflakeIdentifiers.validate_single_identifier("") == False
