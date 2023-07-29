from __future__ import annotations

from typing import Generator
import re

import ereport
import snowflake.connector as sf
from empire_commons.types_ import JsonType
from ejson.facades.orjson_ import loads

from esql._internal.ref import REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME


LOGGER = ereport.get_or_make_reporter(REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME)

class SFQueryCollectors:
    @staticmethod
    def gather_all_records(cursor: sf.DictCursor, unused: int) -> list[JsonType]:
        return SFQueryCollectors._parse_results_to_json(cursor.fetchall())

    @staticmethod
    def make_generator(cursor: sf.DictCursor, batch_size: int) -> Generator[JsonType]:
        LOGGER.info('Using generator collector of size %d', batch_size)
        buffer: list[JsonType] = []
        for batch in SFQueryCollectors._parse_results_to_json(cursor.fetchmany(batch_size)):
            print(len(batch))
            buffer.append(batch)
            yield buffer

            buffer = []

    @staticmethod
    def fetch_one_record(cursor: sf.DictCursor, unused: int) -> JsonType:
        return SFQueryCollectors._parse_results_to_json(cursor.fetchone())

    @staticmethod
    def fetch_n_records(cursor: sf.DictCursor, quantity: int) -> list[JsonType]:
        return SFQueryCollectors._parse_results_to_json(cursor.fetchmany(quantity))

    @staticmethod
    def _parse_results_to_json(results: list[dict[str, str]] | dict[str, str] | None) -> list[JsonType] | JsonType | None:
        def _try_parse(value: str) -> dict | str:
            try:
                return loads(value)
            except Exception:
                return re.sub(r'\s{2,}', ' ', str(value).replace('\n', ' '))

        if isinstance(results, (list, tuple)):
            return [{key: _try_parse(value) for key, value in row.items()} for row in results]
        elif isinstance(results, dict):
            return {key: _try_parse(value) for key, value in results.items()}
        elif results is None:
            return None
        else:
            return results
