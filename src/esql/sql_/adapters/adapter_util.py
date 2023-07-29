from __future__ import annotations

from functools import cache


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
