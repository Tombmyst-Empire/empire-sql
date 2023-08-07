from __future__ import annotations

from datetime import datetime
from typing import Any

from empire_commons.exceptions.exceptions import UnexpectedTypeException, InvalidValueException
from esql.sql_.adapters.base.base_functions_provider import BaseFunctionsProvider
from esql.sql_.adapters.base.stmt_components.base_column import BaseColumn
from esql.sql_.adapters.snowflake.stmt_components.snowflake_values import SnowflakeValues, SnowflakeValueTypes


class SnowflakeFunctions(BaseFunctionsProvider):
    @staticmethod
    def abs_(numeric_expression: BaseColumn | int | float) -> str:
        """
        Returns the absolute value of a numeric expression.

        https://docs.snowflake.com/en/sql-reference/functions/abs
        """
        SnowflakeFunctions._validate_expression(numeric_expression, int, float)
        return f"ABS({SnowflakeFunctions._get_value(numeric_expression)})"

    @staticmethod
    def acos(real_expression: BaseColumn | float) -> str:
        """
        Computes the inverse cosine (arc cosine) of its input; the result is a number in the interval [0, pi].
        :param real_expression: This expression should evaluate to a real number greater than or equal to -1.0 and less than or equal to +1.0.
        :return: The data type of the return value is FLOAT. Returns the arc cosine in radians (not degrees) as a value in the range [0, pi].

        https://docs.snowflake.com/en/sql-reference/functions/acos
        """
        SnowflakeFunctions._validate_expression(real_expression, float)
        if isinstance(real_expression, float) and -1.0 > real_expression > 1.0:
            raise InvalidValueException('real_expression', real_expression, 'between -1.0 and 1.0')

        return f"ACOS({SnowflakeFunctions._get_value(real_expression)})"

    @staticmethod
    def acosh(real_expression: BaseColumn | float) -> str:
        """
        Computes the inverse (arc) hyperbolic cosine of its input.
        :param real_expression: This expression should evaluate to a FLOAT number greater than or equal to 1.0.
        :return: The data type of the returned value is FLOAT.

        https://docs.snowflake.com/en/sql-reference/functions/acosh
        """
        SnowflakeFunctions._validate_expression(real_expression, float)
        if isinstance(real_expression, float) and -1.0 > real_expression > 1.0:
            raise InvalidValueException('real_expression', real_expression, 'between -1.0 and 1.0')

        return f"ACOSH({SnowflakeFunctions._get_value(real_expression)})"

    @staticmethod
    def add_months(date_or_timestamp_expression: BaseColumn | datetime, number_of_months_expression: BaseColumn | int | float) -> str:
        """
        Computes the inverse (arc) hyperbolic cosine of its input.
        :param date_or_timestamp_expression: This expression should evaluate to a FLOAT number greater than or equal to 1.0.
        :return: The data type of the returned value is FLOAT.

        https://docs.snowflake.com/en/sql-reference/functions/acosh
        """
        SnowflakeFunctions._validate_expression(date_or_timestamp_expression, datetime)
        SnowflakeFunctions._validate_expression(number_of_months_expression, int, float)

        return f"ADD_MONTHS({SnowflakeFunctions._get_value(date_or_timestamp_expression)}, {SnowflakeFunctions._get_value(number_of_months_expression)})"

    @staticmethod
    def _validate_expression(expression: BaseColumn | Any, *types: type):
        if (
                isinstance(expression, BaseColumn) and not expression.is_of_type(*types)
        ) or (
            not isinstance(expression, *types)
        ):
            raise UnexpectedTypeException(
                f'Expression {expression}',
                f'One of: {", ".join([type_.__name__ for type_ in types])}',
                expression.type_ if isinstance(expression, BaseColumn) else type(expression)
            )

    @staticmethod
    def _get_value(expression: BaseColumn | Any, *, should_parse_json: bool = False) -> str:
        return expression.build() if isinstance(expression, BaseColumn) else SnowflakeValues.prepare_value_by_deducing_python_type(expression, should_parse_json=should_parse_json)