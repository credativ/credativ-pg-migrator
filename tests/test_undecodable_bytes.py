# SPDX-License-Identifier: GPL-3.0-or-later
"""
A byte which the assumed encoding cannot read is never deleted from a value.

P1-1 of development/OPEN_ISSUES.md: the MS SQL Server connector decoded the values pyodbc
hands over as bytes with `errors='ignore'` in three places, so a byte which did not fit the
assumed encoding was removed from the value. The row reached the target shorter than it left
the source and nothing said so - not the row counts, and not the validator, which reads both
sides through the same decoder.

What is asserted here is the whole of the repair:

  * the decision, `migration.on_undecodable_bytes`, in all three of its settings;
  * that the default never loses a byte - what latin1 reads can be encoded back to exactly
    the bytes which were read;
  * that `fail` refuses the value rather than guessing, and says so in a message which names
    the setting;
  * that `remove`, the behaviour from before the repair, is still reachable and is now
    reported for every value it happens to;
  * that a value one of the expected encodings can read is decoded and NOT reported, because
    a line per row would bury the values which really did not fit;
  * and the connector itself: the ODBC converters go through the decision, the summary is
    written when the connection is closed, and a datetimeoffset which is not the 20 byte
    structure is read as text rather than as the repr of its bytes.

Nothing here connects to anything.

Run with:  python3 -m pytest tests/test_undecodable_bytes.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import text_decoding
from credativ_pg_migrator.text_decoding import TextDecoder, UndecodableBytes


## A value in Windows-1252, the encoding of a SQL Server database old enough to be migrated.
## It is not utf-8 - 0xe9 is not a valid start byte on its own - and its length is odd, so it
## is not utf-16 either: neither of the encodings which are expected for it can read it, which
## is the case the setting decides.
CP1252 = 'café!'.encode('cp1252')

## The same shape, longer, so that the hexadecimal preview has something to cut.
CP1252_LONG = 'Bäckerei Müller, Straße 17, Köln!'.encode('cp1252')
assert len(CP1252) % 2 and len(CP1252_LONG) % 2, 'an even length would be read by utf-16'

## Four bytes of Windows-1252, which utf-16 reads without complaint as two characters of
## another script. No decoder can tell that this is not what was meant - see the module.
CP1252_EVEN = 'café'.encode('cp1252')


class RecordingConfig:
    """A configuration which answers the one setting and collects what was written."""

    def __init__(self, policy='substitute'):
        self.policy = policy
        self.messages = []

    def get_on_undecodable_bytes_action(self):
        return self.policy

    def print_log_message(self, level, message):
        self.messages.append((level, str(message)))

    def levels(self, level):
        return [message for written, message in self.messages if written == level]


def decoder(policy='substitute', **kwargs):
    config = RecordingConfig(policy)
    return TextDecoder(config, 'test', **kwargs), config


# --------------------------------------------------------------------------------------
# what the expected encodings read is not touched and not reported


@pytest.mark.parametrize('value', ['café', 'plain ascii', ''])
def test_a_string_is_handed_back_as_it_is(value):
    text, config = decoder()
    assert text.decode(value) == value
    assert config.messages == []


def test_none_stays_none():
    text, config = decoder()
    assert text.decode(None) is None
    assert config.messages == []


def test_a_value_in_the_first_expected_encoding_is_decoded_without_a_word():
    text, config = decoder()
    assert text.decode('café'.encode('utf-8')) == 'café'
    assert config.messages == []
    assert text.total() == 0


def test_a_value_the_second_expected_encoding_reads_is_counted_and_not_written_per_value():
    """
    Which of utf-8 and utf-16 the ODBC driver hands over depends on how it was built, so both
    are expected and neither is a guess. A message per value would be a message per row - the
    count is what the summary reports.
    """
    text, config = decoder()
    assert text.decode('café'.encode('utf-16')) == 'café'
    assert config.messages == []
    assert text.total() == 1
    text.log_summary()
    written = config.levels('INFO')[0]
    assert '1 read as utf-16' in written
    assert 'Nothing was guessed and no byte was lost' in written
    assert config.levels('WARNING') == []


def test_utf_16_reads_a_value_of_even_length_which_is_not_utf_16_and_is_counted_for_it():
    """
    The limit of the detection, asserted so that it is recorded rather than discovered: four
    bytes of Windows-1252 are read by utf-16 as two characters nobody wrote. The count in the
    summary is the only evidence there is - and before the repair there was none at all.
    """
    text, config = decoder()
    decoded = text.decode(CP1252_EVEN, place='a column')
    assert decoded != 'café'
    assert decoded.encode('utf-16-le') == CP1252_EVEN, 'no byte is lost either way'
    assert text.summary() == {'test: a column': {'read as utf-16': 1}}


def test_bytearray_and_memoryview_are_read_like_bytes():
    text, _ = decoder()
    assert text.decode(bytearray('café'.encode('utf-8'))) == 'café'
    assert text.decode(memoryview('café'.encode('utf-8'))) == 'café'


def test_something_which_is_not_bytes_at_all_is_stringified():
    text, _ = decoder()
    assert text.decode(17) == '17'


# --------------------------------------------------------------------------------------
# substitute - the default


def test_the_default_loses_no_byte():
    """The point of the repair: what comes out can be encoded back to what went in."""
    text, config = decoder()
    decoded = text.decode(CP1252)
    assert decoded.encode('latin1') == CP1252
    assert len(decoded) == len(CP1252)
    assert config.levels('WARNING'), 'a substituted value must be reported'


def test_the_default_is_substitute_when_the_configuration_does_not_know_the_setting():
    class Older:
        def print_log_message(self, level, message):
            pass

    text = TextDecoder(Older(), 'test')
    assert text.policy == text_decoding.DEFAULT_POLICY == 'substitute'
    assert text.decode(CP1252).encode('latin1') == CP1252


def test_the_report_names_the_place_the_offset_and_no_value():
    text, config = decoder()
    text.decode(CP1252_LONG, place='SQL type -152 (xml)')
    written = config.levels('WARNING')[0]
    assert 'SQL type -152 (xml)' in written
    assert 'latin1' in written
    assert 'offset' in written
    ## the value itself is data - only a hexadecimal window of it may be shown
    assert 'Bäckerei' not in written


def test_u_fffd_is_never_written():
    """
    The replacement character cannot be told apart from one which was in the data and cannot
    be turned back into the byte it stood for. It is what P1-2 is about and it is not an
    answer this module gives.
    """
    text, config = decoder()
    for policy in text_decoding.POLICIES:
        text, config = decoder(policy)
        try:
            decoded = text.decode(CP1252)
        except UndecodableBytes:
            continue
        assert '�' not in decoded


def test_without_a_last_resort_encoding_the_bytes_are_escaped_and_not_dropped():
    text, config = decoder(last_resort=None)
    decoded = text.decode(CP1252)
    assert decoded == 'caf\\xe9!'
    assert decoded.encode('utf-8').decode('unicode_escape') == CP1252.decode('latin1')
    assert 'escape' in config.levels('WARNING')[0]
    assert text.summary() == {'test': {'with bytes escaped': 1}}


# --------------------------------------------------------------------------------------
# fail


def test_fail_refuses_the_value_and_says_which_setting_did_it():
    text, config = decoder('fail')
    with pytest.raises(UndecodableBytes) as raised:
        text.decode(CP1252, place='SQL type -152 (xml)')
    assert text.summary() == {'test: SQL type -152 (xml)': {'refused': 1}}
    message = str(raised.value)
    assert 'SQL type -152 (xml)' in message
    assert 'on_undecodable_bytes' in message
    assert 'utf-8 or utf-16' in message


def test_fail_leaves_a_value_the_expected_encodings_can_read_alone():
    text, _ = decoder('fail')
    assert text.decode('café'.encode('utf-8')) == 'café'


# --------------------------------------------------------------------------------------
# remove - what the code did before, now reported


def test_remove_still_deletes_the_byte_and_now_reports_it():
    text, config = decoder('remove')
    decoded = text.decode(CP1252)
    assert decoded == 'caf!'
    written = config.levels('WARNING')[0]
    assert 'DELETED' in written
    assert "'remove'" in written
    assert text.summary() == {'test': {'with bytes deleted': 1}}


def test_remove_is_what_the_connector_used_to_do():
    """The old line, so that the setting is measured against it and not against a memory."""
    text, _ = decoder('remove')
    assert text.decode(CP1252) == CP1252.decode('utf-8', errors='ignore')


# --------------------------------------------------------------------------------------
# counting and reporting


def test_the_detailed_reports_are_limited_and_the_rest_are_counted():
    text, config = decoder(detailed_reports=2)
    for _ in range(10):
        text.decode(CP1252, place='one column')
    warnings = config.levels('WARNING')
    ## two values in full, then one line saying where the rest went
    assert len(warnings) == 3
    assert 'DEBUG' in warnings[-1]
    assert len(config.levels('DEBUG')) == 8
    assert text.total() == 10


def test_the_summary_counts_every_place_separately():
    text, config = decoder()
    text.decode(CP1252, place='SQL type -152 (xml)')
    text.decode(CP1252, place='SQL type -150 (sql_variant)')
    text.decode(CP1252, place='SQL type -150 (sql_variant)')
    assert text.summary() == {
        'test: SQL type -152 (xml)': {'read as latin1 as a last resort': 1},
        'test: SQL type -150 (sql_variant)': {'read as latin1 as a last resort': 2},
    }
    assert text.total() == 3


def test_the_summary_is_silent_when_every_value_fitted():
    text, config = decoder()
    text.decode('café'.encode('utf-8'))
    text.log_summary()
    assert config.messages == []


def test_the_summary_names_the_setting_which_was_applied():
    text, config = decoder()
    text.decode(CP1252, place='SQL type -152 (xml)')
    config.messages.clear()
    text.log_summary()
    written = config.levels('WARNING')[0]
    assert 'SQL type -152 (xml)' in written
    assert '1 read as latin1 as a last resort' in written
    assert "'substitute'" in written


# --------------------------------------------------------------------------------------
# the connector


def ms_sql_connector_class():
    """The connector, or a skip - it imports its drivers when the module is read."""
    try:
        from credativ_pg_migrator.connectors.ms_sql_connector import MsSQLConnector
    except Exception as error:
        pytest.skip(f"ms_sql needs a driver which is not installed here ({error})")
    return MsSQLConnector


@pytest.fixture
def connector():
    made = ms_sql_connector_class().__new__(ms_sql_connector_class())
    made.config_parser = RecordingConfig()
    made.connection = None
    return made


def test_the_connector_decodes_an_odbc_value_without_losing_a_byte(connector):
    decoded = connector.decode_odbc_value(CP1252, -152)
    assert decoded.encode('latin1') == CP1252
    written = connector.config_parser.levels('WARNING')[0]
    assert 'ms_sql_connector: SQL type -152 (xml)' in written


def test_the_connector_reads_a_value_with_a_byte_order_mark_as_utf_16(connector):
    """
    utf-8 is tried first for everything else, and a value which starts with a BOM is utf-16
    whatever the driver was built for - so that encoding goes first for it.
    """
    value = '﻿' + 'café'
    assert connector.decode_odbc_value(value.encode('utf-16'), -152) == value
    assert connector.config_parser.messages == []


def test_every_byte_valued_odbc_type_is_named_in_a_message(connector):
    """
    The converter is registered per type code and knows nothing else about where the value
    stood, so the name of the type is the whole of what a message can say about it.
    """
    registered = (-155, -154, -152, -151, -150)
    assert set(registered) <= set(ms_sql_connector_class().ODBC_TYPE_NAMES)
    for type_code in registered:
        connector.config_parser.messages.clear()
        connector.decode_odbc_value(CP1252, type_code)
        written = connector.config_parser.levels('WARNING')[0]
        assert f"SQL type {type_code} " in written
        assert 'unknown' not in written


def test_a_type_code_nobody_registered_is_still_reported(connector):
    connector.decode_odbc_value(CP1252, -99)
    assert 'SQL type -99 (unknown)' in connector.config_parser.levels('WARNING')[0]


def test_every_registered_converter_is_callable_with_one_argument(monkeypatch):
    """
    pyodbc calls an output converter with the value alone, so the type code each of them is
    registered for is bound where it is registered. A converter which cannot be called that
    way would raise on the first row of the first table and on nothing before it - there is
    no other test in the tree which opens a connection.
    """
    connector_class = ms_sql_connector_class()
    from credativ_pg_migrator.connectors import ms_sql_connector as module

    registered = {}

    class Connection:
        autocommit = False

        def add_output_converter(self, type_code, converter):
            registered[type_code] = converter

    class Config(RecordingConfig):
        def get_connectivity(self, direction):
            return 'odbc'

        def get_connect_string(self, direction):
            return 'DRIVER={nothing};'

    monkeypatch.setattr(module.pyodbc, 'connect', lambda *a, **k: Connection())

    made = connector_class.__new__(connector_class)
    made.config_parser = Config()
    made.source_or_target = 'source'
    made.connect()

    assert sorted(registered) == [-155, -154, -152, -151, -150]

    ## the wide types: a value which no expected encoding reads keeps every byte
    for type_code in (-154, -152, -150):
        assert registered[type_code](CP1252).encode('latin1') == CP1252
    ## the user defined type is handed over as the bytes it is, undecoded
    assert registered[-151](CP1252) == CP1252
    ## and the 20 byte structure of a datetimeoffset is still read as the timestamp it is
    made_at = struct_datetimeoffset(2026, 8, 24, 9, 30, 0, 123456, 2, 0)
    assert registered[-155](made_at).startswith('2026-08-24 09:30:00.123456+02:00')
    assert all(registered[type_code](None) is None for type_code in registered)


def struct_datetimeoffset(year, month, day, hour, minute, second, microsecond,
                          tz_hour, tz_minute):
    """The 20 bytes SQL Server sends for a datetimeoffset."""
    import struct

    return struct.pack("<hhhhhhIhh", year, month, day, hour, minute, second,
                       microsecond * 1000, tz_hour, tz_minute)


def test_a_datetimeoffset_which_is_not_the_20_byte_structure_is_read_as_text(connector):
    """
    It used to be str(value) when utf-8 could not read it, which writes the repr of the bytes
    - b'...' - into the target as if it were the value.
    """
    decoded = connector.decode_odbc_value(CP1252, -155)
    assert not decoded.startswith("b'")
    assert decoded.encode('latin1') == CP1252


def test_the_connector_writes_the_summary_when_the_connection_is_closed(connector):
    connector.decode_odbc_value(CP1252, -152)
    connector.config_parser.messages.clear()
    connector.disconnect()
    assert any('read as latin1 as a last resort' in message
               for message in connector.config_parser.levels('WARNING'))


def test_a_connector_which_read_nothing_odd_says_nothing_when_it_closes(connector):
    connector.decode_odbc_value('café'.encode('utf-8'), -152)
    connector.config_parser.messages.clear()
    connector.disconnect()
    assert connector.config_parser.messages == []


def test_the_source_of_the_connector_holds_no_lenient_decode_any_more():
    """
    The three lines P1-1 names. A `errors='ignore'` added back here would delete bytes again
    without anything else in the suite noticing.
    """
    path = os.path.join(REPO, 'credativ_pg_migrator', 'connectors', 'ms_sql_connector.py')
    with open(path, encoding='utf-8') as handle:
        code = [line for line in handle if not line.lstrip().startswith('#')]
    for lenient in ("errors='ignore'", "errors='replace'", 'errors="ignore"', 'errors="replace"'):
        offenders = [line.strip() for line in code if lenient in line]
        assert not offenders, f'{lenient} is back in ms_sql_connector.py: {offenders}' 
