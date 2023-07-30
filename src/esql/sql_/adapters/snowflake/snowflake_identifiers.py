from __future__ import annotations

from dataclasses import dataclass

import ereport
from empire_commons import list_util
from empire_commons.functions import or_raise_broad, DefferedCall, then, get_

from esql._internal.ref import REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME, DEFAULT_REPORTER
from esql.exceptions import BadIdentifierException

LOGGER = DEFAULT_REPORTER


@dataclass(frozen=True, slots=True)
class SnowflakeIdentifierComponents:
    database_name: str | None = None
    schema_name: str | None = None
    object_name: str | None = None


class SnowflakeIdentifiers:
    @staticmethod
    def validate_single_identifier(identifier: str) -> bool:
        if identifier and len(identifier) > 255:
            raise BadIdentifierException(identifier, f'Identifier is too long: {len(identifier)} / 255')
        elif not identifier:
            return False

        return True

    @staticmethod
    def make_qualified_name(object_name: str, schema_name: str | None, database_name: str | None) -> str:
        components: list[str] = list(filter(None, [
            SnowflakeIdentifiers.format_identifier(
                then(
                    DefferedCall(SnowflakeIdentifiers.validate_single_identifier, database_name),
                    DefferedCall(get_, database_name),
                    break_at_first_false=True,
                    value_to_return_on_break=None
                )
            ),
            SnowflakeIdentifiers.format_identifier(
                then(
                    DefferedCall(SnowflakeIdentifiers.validate_single_identifier, schema_name),
                    DefferedCall(get_, schema_name),
                    break_at_first_false=True,
                    value_to_return_on_break=None
                )
            ),
            SnowflakeIdentifiers.format_identifier(
                then(
                    DefferedCall(SnowflakeIdentifiers.validate_single_identifier, object_name),
                    DefferedCall(get_, object_name),
                    break_at_first_false=True,
                    value_to_return_on_break=None
                )
            )
        ]))
        joined_components: str = '"."'.join(components)
        return f'"{joined_components}"'

    @staticmethod
    def make_qualified_name_dataclass(components: SnowflakeIdentifierComponents) -> str:
        return SnowflakeIdentifiers.make_qualified_name(components.database_name, components.schema_name, components.object_name)

    @staticmethod
    def format_identifier(identifier: str | None) -> str:
        if not identifier or (identifier.startswith('"') and identifier.endswith('"')):
            return identifier
        elif '"' in identifier:
            identifier = identifier.replace('"', '""')

        return f'"{identifier.upper()}"'

    @staticmethod
    def format_qualified_name(qualified_name: str) -> str:
        return '.'.join([
            SnowflakeIdentifiers.format_identifier(identifier) for identifier in qualified_name.split('.')
        ])

    @staticmethod
    def extract_components(value: str, *, starts_with_db: bool, starts_with_schema: bool) -> SnowflakeIdentifierComponents:
        if '"."' in value:
            split_name: list[str] = value.split('"."')
            split_name[0] = split_name[0][1:]
            split_name[-1] = split_name[-1][:-1]
        elif '.' in value:
            split_name: list[str] = value.split('.')
        else:
            if starts_with_db:
                return SnowflakeIdentifierComponents(database_name=value)
            elif starts_with_schema:
                return SnowflakeIdentifierComponents(schema_name=value)
            else:
                return SnowflakeIdentifierComponents(object_name=value)

        if starts_with_db or (starts_with_schema and len(split_name) == 3):
            return SnowflakeIdentifierComponents(
                database_name=split_name[0],
                schema_name=list_util.try_get(split_name, 1),
                object_name=list_util.try_get(split_name, 2)
            )
        elif starts_with_schema:
            return SnowflakeIdentifierComponents(
                schema_name=split_name[0],
                object_name=list_util.try_get(split_name, 1)
            )
        elif len(split_name) == 3:
            return SnowflakeIdentifierComponents(
                database_name=split_name[0],
                schema_name=split_name[1],
                object_name=split_name[2]
            )
        elif len(split_name) == 1:
            return SnowflakeIdentifierComponents(
                object_name=split_name[0]
            )
        else:
            raise ValueError(f'Unable to parse the following: {value} with parameters "starts_with_db": {starts_with_db} and "starts_with_schema": {starts_with_schema}')

