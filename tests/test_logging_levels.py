# SPDX-License-Identifier: GPL-3.0-or-later
"""
Behaviour of --log-level and ConfigParser.print_log_message.

The levels are a severity ladder: a message is written when its severity is at least the
severity of the level the run was started with. The default level therefore shows
warnings, which it did not until 0.16.1 - warnings say what a migration could not
convert, so hiding them by default hid exactly what had to be read.

Run with:  python3 -m pytest tests/test_logging_levels.py -v
"""

import os
import sys
import types

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.constants import MigratorConstants

ALL_LEVELS = MigratorConstants.get_message_levels()


class RecordingLogger:
    """Stands in for the logging.Logger, remembering which method was called."""

    def __init__(self):
        self.records = []

    def error(self, message):
        self.records.append(('error', message))

    def warning(self, message):
        self.records.append(('warning', message))

    def info(self, message):
        self.records.append(('info', message))

    def debug(self, message):
        self.records.append(('debug', message))


def parser_at(log_level):
    """A ConfigParser wired for logging only - print_log_message needs nothing else."""
    parser = ConfigParser.__new__(ConfigParser)
    parser.logger = RecordingLogger()
    parser.args = types.SimpleNamespace(log_level=log_level)
    return parser


def levels_written(log_level):
    parser = parser_at(log_level)
    for level in ALL_LEVELS:
        parser.print_log_message(level, f'message from {level}')
    return [level for level in ALL_LEVELS
            if any(f'message from {level}' == message or message.endswith(f'message from {level}')
                   for _, message in parser.logger.records)]


# --------------------------------------------------------------------------------------
# the ladder


def test_the_default_level_shows_warnings():
    """The regression this test exists for: a WARNING used to be invisible by default."""
    assert 'WARNING' in levels_written(None)


def test_the_default_is_info():
    assert levels_written(None) == levels_written('INFO')


@pytest.mark.parametrize('log_level,expected', [
    ('ERROR',   ['ERROR']),
    ('WARNING', ['ERROR', 'WARNING']),
    ('INFO',    ['ERROR', 'WARNING', 'INFO']),
    ('DEBUG',   ['ERROR', 'WARNING', 'INFO', 'DEBUG']),
    ('DEBUG2',  ['ERROR', 'WARNING', 'INFO', 'DEBUG', 'DEBUG2']),
    ('DEBUG3',  ['ERROR', 'WARNING', 'INFO', 'DEBUG', 'DEBUG2', 'DEBUG3']),
])
def test_each_level_shows_itself_and_everything_more_severe(log_level, expected):
    assert levels_written(log_level) == expected


def test_each_level_is_a_superset_of_the_quieter_one():
    """Turning the level up may only add messages, never remove them."""
    previous = set()
    for log_level in ALL_LEVELS:
        written = set(levels_written(log_level))
        assert previous <= written, f'{log_level} lost messages the quieter level showed'
        previous = written


def test_errors_are_shown_at_every_level():
    for log_level in ALL_LEVELS:
        assert 'ERROR' in levels_written(log_level)


# --------------------------------------------------------------------------------------
# routing and robustness


@pytest.mark.parametrize('level,expected_method', [
    ('ERROR', 'error'),
    ('WARNING', 'warning'),
    ('INFO', 'info'),
    ('DEBUG', 'debug'),
    ('DEBUG2', 'debug'),
    ('DEBUG3', 'debug'),
])
def test_each_level_uses_the_matching_logger_method(level, expected_method):
    """A warning must reach logger.warning, so the log renders it as [WARNING]."""
    parser = parser_at('DEBUG3')
    parser.print_log_message(level, 'text')
    assert parser.logger.records[0][0] == expected_method


def test_info_is_not_prefixed_with_its_own_level():
    """The formatter already prints [INFO]; the message used to repeat it."""
    parser = parser_at('INFO')
    parser.print_log_message('INFO', 'plain text')
    assert parser.logger.records[0][1] == 'plain text'


@pytest.mark.parametrize('level', ['DEBUG2', 'DEBUG3'])
def test_the_extra_debug_levels_are_marked(level):
    """DEBUG2 and DEBUG3 have no counterpart in logging, so they say which they are."""
    parser = parser_at('DEBUG3')
    parser.print_log_message(level, 'text')
    assert parser.logger.records[0][1] == f'{level}: text'


@pytest.mark.parametrize('written', ['info', 'Info', 'WARNING', 'warning'])
def test_the_level_name_is_case_insensitive(written):
    parser = parser_at('INFO')
    parser.print_log_message(written, 'text')
    assert parser.logger.records


def test_an_unknown_message_level_is_refused():
    with pytest.raises(ValueError):
        parser_at('INFO').print_log_message('CRITICAL', 'text')


def test_an_unusable_log_level_falls_back_to_info_rather_than_silencing_the_run():
    parser = parser_at('NOT_A_LEVEL')
    parser.print_log_message('WARNING', 'text')
    parser.print_log_message('DEBUG3', 'text')
    assert [method for method, _ in parser.logger.records] == ['warning']


# --------------------------------------------------------------------------------------
# the constants


def test_levels_are_listed_quietest_first():
    severities = [MigratorConstants.get_message_level_severity(level) for level in ALL_LEVELS]
    assert severities == sorted(severities, reverse=True)


def test_every_listed_level_has_a_severity():
    assert all(MigratorConstants.get_message_level_severity(level) is not None
               for level in ALL_LEVELS)


def test_an_unknown_level_has_no_severity():
    assert MigratorConstants.get_message_level_severity('CRITICAL') is None
    assert MigratorConstants.get_message_level_severity(None) is None
