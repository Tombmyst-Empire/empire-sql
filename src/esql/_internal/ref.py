from __future__ import annotations

from typing import Final

import ereport

REPORTER_NAME: Final[str] = 'esql'
LOGGING_LEVEL_ENV_VAR_NAME: Final[str] = 'E_SQL_LOGGING_LEVEL'

DEFAULT_REPORTER: Final[ereport.Reporter] = ereport.get_or_make_reporter(REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME)
