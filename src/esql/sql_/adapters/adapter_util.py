from __future__ import annotations

from functools import cache
from empire_commons.regex_util import RegexUtil
from typing import Any


@cache
def indent_level_as_tabs(indent_level: int) -> str:
    """
    Returns a *indent_level* times ``\\t`` string
    """
    if indent_level <= 0:
        return ''

    return indent_level * '\t'


def format_query(query: str, indent_level: int) -> str:
    return f'{indent_level_as_tabs(indent_level)}{query}'


def escape_unescaped_quotes_in_string(string: str, quote: str = "'") -> str:
    regex = RegexUtil.get_compiled_regex(f'(?<!\\\\){quote}')
    return regex.sub(f'\\{quote}', string)
