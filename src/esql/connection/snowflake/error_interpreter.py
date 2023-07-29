import re

import ereport
import snowflake.connector as sf
from econsole.styles import Color4Bits, ConsoleCharacters as Console
from empire_commons.list_util import try_get
from empire_commons.string_util import StringUtil
from esql._internal.ref import REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME

PROGRAMMING_ERROR_LINE_POS_EXTRACTOR_REGEX = re.compile(r'error line (\d+) at position (\d+)')
PROGRAMMING_ERROR_LINE_POS_UNEXPECTED_REGEX = re.compile(r"unexpected '(\w+)'")


LOGGER = ereport.get_or_make_reporter(REPORTER_NAME, LOGGING_LEVEL_ENV_VAR_NAME)


def interpret_programming_error(query: str, exception: sf.errors.ProgrammingError):
    if 'sql execution canceled' in exception.msg:
        LOGGER.fatal('SQL execution was cancelled')
        return

    LOGGER.error('Programming error: %s', exception.msg.replace('\n', ' '))

    try:
        line, pos = PROGRAMMING_ERROR_LINE_POS_EXTRACTOR_REGEX.findall(exception.msg)[0]
    except IndexError as error:
        print(query)
        return

    unexpected: str = try_get(PROGRAMMING_ERROR_LINE_POS_UNEXPECTED_REGEX.findall(exception.msg), 0, ' ')

    line = int(line)
    pos = int(pos)
    position_in_string: int = StringUtil.index_of(query, '\n', line - 1)
    print(query)
    query = query[position_in_string - (pos * 2):position_in_string + (pos * 2)] + \
            Console.set_slow_blink() + \
            Console.set_foreground_4bits(Color4Bits.DARK_RED) + \
            '\n' + \
            ('.' * (pos -1)) + \
            Console.set_bold() + \
            Console.set_foreground_4bits(Color4Bits.RED) + \
            '^' * (len(unexpected)) + \
            '\n' + \
            Console.reset()
    print(query)
