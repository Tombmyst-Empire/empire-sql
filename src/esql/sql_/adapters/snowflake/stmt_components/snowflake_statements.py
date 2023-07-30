from __future__ import annotations

from typing import Any, Protocol

from ejson.facades.orjson_ import dumps
from empire_commons.exceptions import UnexpectedTypeException
from empire_commons.functions import coalesce
from empire_commons import list_util
from esql.sql_.adapters.adapter_util import format_query, escape_unescaped_quotes_in_string
from esql.sql_.adapters.snowflake.stmt_components.snowflake_identifiers import SnowflakeIdentifiers
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValueTypes, SnowflakeValues


class StatementProtocol(Protocol):
    def get(self) -> Any: ...


def prepare_value(value: Any, value_type: SnowflakeValueTypes) -> str:
    if value_type == SnowflakeValueTypes.IDENTIFIER:
        if not isinstance(value, str):
            raise UnexpectedTypeException(
                str(value), str.__name__, type(value).__name__
            )
        return SnowflakeIdentifiers.format_qualified_name(value)
    elif value_type == SnowflakeValueTypes.VALUE:
        return SnowflakeValues.prepare_value_by_deducing_python_type(value)
    elif value_type == SnowflakeValueTypes.VALUE_JSON:
        return f"PARSE_JSON('{escape_unescaped_quotes_in_string(dumps(value))}')"
    elif value_type == SnowflakeValueTypes.TO_STRING:
        return f"'{str(value)}'"
    elif value_type == SnowflakeValueTypes.AS_IS:
        return str(value)
    else:
        raise UnexpectedTypeException(str(value_type), SnowflakeValueTypes.__name__, type(value_type).__name__)


class StatementElement:
    """
    An element of a statement.

    *arg_string* can contain the string ``%%``, which will be replaced
    by *value*.

    When getting the constructed arg, will be returned *arg_string*, in which,
    if applicable, will be replaced ``%%`` by *value*, only if *value* evaluates
    to a truthy value, otherwise will be returned *arg_on_not_value*
    """
    __slots__ = (
        '_element_string',
        '_value',
        '_element_on_not_value',
        '_value_type'
    )

    def __init__(
            self,
            element_string: str,
            value: Any,
            element_on_not_value: Any = None,
            value_type: SnowflakeValueTypes = SnowflakeValueTypes.IDENTIFIER
    ):
        self._element_string: str = element_string
        self._value: Any = value
        self._element_on_not_value: Any = element_on_not_value
        self._value_type: SnowflakeValueTypes = value_type

    def get(self) -> Any:
        return self._element_string.replace(
            '%%',
            prepare_value(self._value, self._value_type),
            1
        ) if self._value else self._element_on_not_value


class StatementElementMulti:
    """
    Multiple substitution element.

    Substitution tokens in *element_string* should be %N where N is an integer >= 0.

    Provided *values* should be a tuple of length 2 or 3:

    - 0: the substitution string
    - 1: a value, that will be evaluated (on truthy, substitution will occur). If #0
        contains %%, it will be replaced by this value
    - 2: when provided, the value to insert when #1 is falsy (no substitution). Defaults to empty string.

    Example:

        StatementElementMulti(
            'APPLICATION ROLE %0%1',
            ('%%.', application_name),
            ('%%', application_role)
        )

    Produces:

        ``application_name = 'my_application'``
        ``application_role = 'my_role'``
        'APPLICATION ROLE my_application.my_role'

    Or:

        ``application_name = None``
        ``application_role = 'my_role``
        'APPLICATION ROLE my_role'
    """
    __slots__ = (
        '_element_string',
        '_values',
        '_value_type'
    )

    def __init__(
            self,
            element_string: str,
            *values: tuple[str, Any] | tuple[str, Any, Any],
            value_type: SnowflakeValueTypes = SnowflakeValueTypes.IDENTIFIER
    ):
        self._element_string: str = element_string
        self._values: tuple[tuple[str, Any] | tuple[str, Any, Any]] = values
        self._value_type: SnowflakeValueTypes = value_type

    def get(self) -> str:
        prepared_values: list[str] = [
            a_value[0].replace(
                '%%',
                prepare_value(
                    a_value[1],
                    self._value_type
                ),
                1
            ) if a_value[1] else list_util.try_get(list(a_value), 2, '')
            for a_value in self._values
        ]

        prepared_element: str = self._element_string
        for index, a_value in enumerate(prepared_values):
            if f'%{index}' in prepared_element:
                prepared_element = prepared_element.replace(
                    f'%{index}',
                    a_value if a_value else ''
                )

        return prepared_element.strip()


class StatementElementFirst:
    """
    Takes the first non None provided value as element. If all is None,
    produces an empty string.
    """
    __slots__ = (
        '_element_candidates',
        '_value_type'
    )

    def __init__(
            self,
            *candidates: StatementProtocol,
            value_type: SnowflakeValueTypes = SnowflakeValueTypes.IDENTIFIER
    ):
        self._element_candidates: tuple[StatementProtocol, ...] = candidates
        self._value_type: SnowflakeValueTypes = value_type

    def get(self) -> Any:
        return coalesce(
            *(element.get() for element in self._element_candidates)
        )


def build_statement(*args: StatementProtocol | str, indent_level: int) -> str:
    def _handle_arg(an_arg: StatementProtocol | str) -> Any:
        if isinstance(an_arg, StatementElement):
            return an_arg.get()
        else:
            return an_arg

    return format_query(
        ' '.join(filter(None, [
            _handle_arg(arg) for arg in args
        ])),
        indent_level
    ).strip()
