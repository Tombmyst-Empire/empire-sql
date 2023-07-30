from __future__ import annotations

from esql.sql_.adapters.adapter_util import escape_unescaped_quotes_in_string


def test_basic_single_quote_escaping():
    assert escape_unescaped_quotes_in_string("This is a 'quote'") == "This is a \\'quote\\'"

def test_basic_double_quote_escaping():
    assert escape_unescaped_quotes_in_string('This is a "quote"', quote='"') == 'This is a \\"quote\\"'

def test_multiple_unescaped_quotes():
    assert escape_unescaped_quotes_in_string("Quotes 'here' and 'there'") == "Quotes \\'here\\' and \\'there\\'"

def test_pre_escaped_quotes_untouched():
    assert escape_unescaped_quotes_in_string("This is an already \\'escaped\\' quote") == "This is an already \\'escaped\\' quote"

def test_empty_string():
    assert escape_unescaped_quotes_in_string("") == ""

def test_string_without_quotes():
    assert escape_unescaped_quotes_in_string("This string has no quotes") == "This string has no quotes"

def test_interleaved_quotes():
    assert escape_unescaped_quotes_in_string("Mix of 'unescaped', \\'escaped\\', and 'more' quotes") == "Mix of \\'unescaped\\', \\'escaped\\', and \\'more\\' quotes"

def test_multiple_backslashes_before_quote():
    assert escape_unescaped_quotes_in_string("This has multiple \\\\\\backslashes\\\\ 'before' a quote") == "This has multiple \\\\\\backslashes\\\\ \\'before\\' a quote"

def test_edge_case_with_only_quote():
    assert escape_unescaped_quotes_in_string("'") == "\\'"

def test_edge_case_with_only_escaped_quote():
    assert escape_unescaped_quotes_in_string("\\'") == "\\'"
