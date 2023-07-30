from __future__ import annotations

from enum import StrEnum


class SnowflakeObjects(StrEnum):
    DATABASE = 'DATABASE'
    EVENT_TABLE = 'EVENT_TABLE'
    FILE_FORMAT = 'FILE_FORMAT'
    FUNCTION = 'FUNCTION'
    PIPE = 'PIPE'
    POLICY = 'POLICY'
    PROCEDURE = 'PROCEDURE'
    SCHEMA = 'SCHEMA'
    SEQUENCE = 'SEQUENCE'
    STREAM = 'STREAM'
    TABLE = 'TABLE'
    TAG = 'TAG'
    TASK = 'TASK'
    VIEW = 'VIEW'
