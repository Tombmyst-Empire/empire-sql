from __future__ import annotations


class BadIdentifierException(Exception):
    def __init__(self, identifier: str, reason: str):
        super().__init__(f'Bad identifier: {identifier}. {reason}')
