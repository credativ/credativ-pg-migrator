# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
What happens to a byte which the assumed encoding cannot read.

A value arrives from a source database as bytes whenever the driver does not know - or does
not say - which encoding it is in, and the migrator has to make a Python string out of it
before it can be written to the target. Nine places did that leniently: three in the MS SQL
Server connector, with `errors='ignore'`, which is where this module comes from:

    value.decode('utf-16', errors='ignore')
    value.decode('latin1', errors='ignore')

`ignore` **deletes** the byte it cannot read. The row arrives in the target shorter than it
left the source, no line is written anywhere, and nothing afterwards can see it: the row
counts match, and the validator compares what it reads on both sides through this same
decoder, so the checksums match as well. Data changed without a trace is the one outcome this
repository treats as worse than a crash. The other six wrote `errors='replace'` instead, which
is the same silence with the replacement character in place of the missing byte.

`errors='strict'` on its own is not the repair either. It would end the migration of a million
rows on the first odd byte, and a value which is not in the encoding the driver assumes is
common in a database old enough to be migrated - so a migrator which cannot get past one is
not usable.

The decision is therefore a setting, `migration.on_undecodable_bytes`, and it is written down
here once for every place which has to make a string out of bytes:

  * `substitute` (the default) - the encodings which are expected for the value are tried in
    order, strictly. If none of them reads it, the last resort encoding does: `latin1` maps
    all 256 byte values one to one, so **no byte is lost** - the characters may be wrong and
    the original bytes are still there to be read again. Every value which needed it is
    counted and reported, with the offset of the byte which did not fit.
  * `fail` - a value which none of the expected encodings can read raises `UndecodableBytes`.
    The read fails, the table or the chunk is recorded as failed through the error path the
    migration already has, and nothing is guessed. This is the setting for a migration which
    must not carry a value nobody has looked at.
  * `remove` - what the code did before this module: the byte is dropped. It stays reachable
    because a migration may already have been run that way, and it is now counted and
    reported for every value it happens to, so it is a decision rather than an accident.

Two things about it are deliberate.

**U+FFFD is never written.** The replacement character cannot be told apart from a U+FFFD
which was in the data and cannot be turned back into the byte it stood for. Writing it was
P1-2 of `development/OPEN_ISSUES.md` - six further places, in the SQLite connector and in the
CSV reader of a file data source - and they were repaired by calling this module rather than
by deciding again. Nine places, one decision. The UNL reader of Informix is not one of them:
it opens its files strictly and refuses such a byte out loud, which is a decision of its own.

**A value which the first expected encoding reads is not reported at all**, and one which a
later one reads is counted but not written per value. Trying utf-8 and then utf-16 for a wide
column is detection rather than guessing - which of the two the driver hands over depends on
how it was built and configured, and both are correct - and a line per row would bury the
values which really did not fit. The counter is what the summary reports.

The detection has a limit which is worth knowing, because this module cannot remove it:
**utf-16 reads almost any byte string of even length**. A value of four bytes in Windows-1252
is not utf-8, is read by utf-16 as two characters of another script, and no decoder anywhere
can tell that this is not what was meant. That is why every value which the first encoding
could not read is counted and named in the summary, per place: the count is the only evidence
there is, and before this module there was none.

A **file** is decided the same way and read differently. A CSV export is opened with the
encoding its data source declares (`format_options.character_set`), so there is no chain
to try: a byte which does not fit means the declaration is wrong or the file is damaged. The
decision is applied by a codec error handler, registered under the name `ERROR_HANDLER` and
handed to `open()`, so the reader of the file is not restructured around it - see
`TextDecoder.open_text()`. `substitute` there keeps the byte as the character `latin1` gives
it and leaves the rest of the file in the declared encoding, which is closer to the file than
decoding the whole of it again would be.

`summary()` returns what happened as counters, per place and per outcome. It is what a
deviations protocol table (P4-6) would be filled from, and it is what `log_summary()` writes
when the connection is closed or the file has been read.
"""

import codecs
import contextlib
import os
import threading

## The values of migration.on_undecodable_bytes.
POLICIES = ('substitute', 'fail', 'remove')
DEFAULT_POLICY = 'substitute'

## How many values are reported in full per place before the rest are counted only. A column
## which is wholly in an unexpected encoding produces one event per row, and a million
## warnings hide the one line which has to be read.
DETAILED_REPORTS_PER_PLACE = 5

## How much of a value a message may show. It is data, so it is shown as hexadecimal, only
## around the byte which did not fit, and never more than this many bytes.
PREVIEW_BYTES = 16


## The decoder a file is being read through, per thread. A codec error handler is given the
## failure and nothing else - not the file, not the setting - so this is where it finds them.
_ACTIVE = threading.local()

## The name open() is given as its `errors`, registered below.
ERROR_HANDLER = 'credativ_pg_migrator_undecodable'


class UndecodableBytes(ValueError):
    """A value none of the expected encodings could read, under `on_undecodable_bytes: fail`."""

    def __init__(self, place, encodings, detail, length, preview):
        self.place = place
        self.encodings = tuple(encodings)
        self.detail = detail
        self.length = length
        self.preview = preview
        super().__init__(
            f"{place}: a value of {length} byte(s) could not be decoded as "
            f"{' or '.join(self.encodings)} - {detail}. The bytes around it are {preview}. "
            f"The value was NOT written to the target and nothing was guessed, because "
            f"migration.on_undecodable_bytes is 'fail'. Set it to 'substitute' to keep such a "
            f"value with the last resort encoding, which loses no byte, and have every "
            f"occurrence reported instead.")


def hex_preview(value, around=None, window=PREVIEW_BYTES):
    """A short hexadecimal window of a value, centred on the byte which did not fit."""
    if not value:
        return '(empty)'
    if around is None:
        start = 0
    else:
        start = max(0, int(around) - window // 2)
    shown = value[start:start + window]
    spelled = ' '.join(f'{byte:02x}' for byte in shown)
    prefix = '... ' if start > 0 else ''
    suffix = ' ...' if start + window < len(value) else ''
    return f'{prefix}{spelled}{suffix} (offset {start})'


def describe_error(error):
    """What the decoder objected to, without the value itself."""
    if error is None:
        return 'the encoding could not read it'
    start = getattr(error, 'start', None)
    end = getattr(error, 'end', None)
    reason = getattr(error, 'reason', None) or str(error)
    obj = getattr(error, 'object', None)
    if start is None:
        return reason
    bad = ''
    if isinstance(obj, (bytes, bytearray)) and end is not None:
        bad = ' (' + ' '.join(f'0x{byte:02x}' for byte in obj[start:end]) + ')'
    return f'{reason} at offset {start}{bad}'


class TextDecoder:
    """
    One decision about undecodable bytes, applied wherever bytes become a string.

    `place` names the code which reads the value and is repeated in every message; a call may
    add to it - the ODBC type code, the column - through the `place` argument of `decode()`.
    `encodings` are the encodings which are expected for the value, tried in order and
    strictly. `last_resort` is the encoding which is used under `substitute` when none of them
    can read the value; `latin1` is the only sensible choice, because it is the only one which
    cannot fail and cannot lose a byte.

    The instance keeps counters and is shared by the threads of one connection, so both are
    taken under a lock. It never holds a value.
    """

    def __init__(self, config_parser, place, encodings=('utf-8', 'utf-16'),
                 last_resort='latin1', policy=None,
                 detailed_reports=DETAILED_REPORTS_PER_PLACE):
        self.config_parser = config_parser
        self.place = place
        self.encodings = tuple(encodings)
        self.last_resort = last_resort
        self.detailed_reports = detailed_reports
        self._policy = policy.strip().lower() if isinstance(policy, str) else None
        self._lock = threading.Lock()
        self._counts = {}
        self._reported = {}

    @property
    def policy(self):
        """The setting, read once. A configuration which does not know it means the default."""
        if self._policy is None:
            reader = getattr(self.config_parser, 'get_on_undecodable_bytes_action', None)
            self._policy = reader() if callable(reader) else DEFAULT_POLICY
        return self._policy

    def decode(self, value, place=None, encodings=None):
        """
        Make a string out of `value`, following `migration.on_undecodable_bytes`.

        `None` and a value which is already a string are handed back untouched, so that a
        caller does not have to ask first. Anything which is not bytes at all is `str()`-ed,
        which is what the callers did before.
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (bytearray, memoryview)):
            value = bytes(value)
        if not isinstance(value, bytes):
            return str(value)

        candidates = tuple(encodings) if encodings else self.encodings
        try:
            return value.decode(candidates[0])
        except UnicodeDecodeError as error:
            first_error = error

        ## Only a value which the first expected encoding could not read costs anything more
        ## than the decode itself - this runs for every text value of every row.
        where = f'{self.place}: {place}' if place else self.place
        for encoding in candidates[1:]:
            try:
                decoded = value.decode(encoding)
            except UnicodeDecodeError:
                continue
            ## Read, but not by the encoding which was expected first. Nothing is written per
            ## value - a column which is wholly in the second encoding is one event per row -
            ## and the count is named in the summary.
            self._count(where, f'read as {encoding}')
            return decoded
        return self._undecodable(value, where, candidates, first_error)

    ## ------------------------------------------------------------------------------------
    ## Files.
    ##
    ## A file is read through a codec error handler rather than value by value, so that the
    ## reader - csv.reader over an open file, in both places which need this - does not have
    ## to be restructured around the decision. The handler is registered once, under a name
    ## which is given to open(); it finds the decoder to report to in `_ACTIVE`, which
    ## `reading()` sets for the current thread.

    @contextlib.contextmanager
    def reading(self, place):
        """Make this decoder the one the codec error handler reports to, in this thread."""
        previous = getattr(_ACTIVE, 'decoder', None)
        previous_place = getattr(_ACTIVE, 'place', None)
        _ACTIVE.decoder, _ACTIVE.place = self, place
        try:
            yield self
        finally:
            _ACTIVE.decoder, _ACTIVE.place = previous, previous_place

    @contextlib.contextmanager
    def open_text(self, path, encoding, place=None, **open_arguments):
        """
        `open()` for a file whose encoding is declared, with the setting applied to the bytes
        which do not fit it.

        Everything read inside the block goes through the decision, including what a
        `csv.reader` built on the handle reads. The block is where the decision applies, so
        the file must be read inside it and not from a handle which outlived it.
        """
        with self.reading(place or os.path.basename(path)):
            with open(path, 'r', encoding=encoding, errors=ERROR_HANDLER,
                      **open_arguments) as handle:
                yield handle

    def _undecodable(self, value, where, candidates, error):
        detail = describe_error(error)
        preview = hex_preview(value, getattr(error, 'start', None))
        tried = ' or '.join(candidates)
        policy = self.policy

        if policy == 'fail':
            self._count(where, 'refused')
            raise UndecodableBytes(where, candidates, detail, len(value), preview)

        if policy == 'remove':
            kept = value.decode(candidates[0], errors='ignore')
            self._count(where, 'with bytes deleted')
            self._report(
                where, 'WARNING',
                f"{where}: a value of {len(value)} byte(s) is not {tried} - {detail}. "
                f"At least that byte was DELETED from the value, which is now {len(kept)} "
                f"character(s), because migration.on_undecodable_bytes is 'remove'. "
                f"The bytes around it are {preview}.")
            return kept

        ## substitute - the default. latin1 cannot fail and cannot lose a byte, so this is
        ## where a value which is in none of the expected encodings normally ends up.
        if self.last_resort:
            try:
                kept = value.decode(self.last_resort)
                self._count(where, f'read as {self.last_resort} as a last resort')
                self._report(
                    where, 'WARNING',
                    f"{where}: a value of {len(value)} byte(s) is not {tried} - {detail}. "
                    f"It was read as {self.last_resort} instead, which keeps every byte and "
                    f"may spell the characters wrongly. The bytes around it are {preview}.")
                return kept
            except UnicodeDecodeError:
                pass

        ## No last resort encoding, or one which cannot read the value either. The bytes are
        ## written as the \xNN escapes of the primary encoding, which keeps them readable
        ## and recoverable rather than deleting them.
        kept = value.decode(candidates[0], errors='backslashreplace')
        self._count(where, 'with bytes escaped')
        self._report(
            where, 'WARNING',
            f"{where}: a value of {len(value)} byte(s) is not {tried} - {detail}, and the "
            f"last resort encoding could not read it either. Every byte which did not fit is "
            f"written as a \\xNN escape, so nothing is lost and the value is not the text it "
            f"was. The bytes around it are {preview}.")
        return kept

    def file_error(self, error, place=None):
        """
        One run of bytes of a file which the declared encoding could not read.

        Answers `(replacement, position to carry on at)`, which is what a codec error handler
        owes its caller. The offset the failure carries is inside the block the reader had
        just read and not inside the file, so it is not reported as a position - the bytes
        themselves are, which is what says what the encoding of the file really is.
        """
        where = f'{self.place}: {place}' if place else self.place
        bad = bytes(error.object[error.start:error.end])
        spelled = ' '.join(f'0x{byte:02x}' for byte in bad)
        policy = self.policy

        if policy == 'fail':
            self._count(where, 'refused')
            raise error

        if policy == 'remove':
            self._count(where, 'with bytes deleted')
            self._report(
                where, 'WARNING',
                f"{where}: {len(bad)} byte(s) ({spelled}) are not {error.encoding} - "
                f"{error.reason}. They were DELETED from the value, because "
                f"migration.on_undecodable_bytes is 'remove'.")
            return ('', error.end)

        ## substitute - the byte is kept as the character latin1 gives it, and the rest of
        ## the file stays in the encoding it was declared as.
        self._count(where, 'read as latin1 as a last resort')
        self._report(
            where, 'WARNING',
            f"{where}: {len(bad)} byte(s) ({spelled}) are not {error.encoding} - "
            f"{error.reason}. They were kept as what latin1 reads them as, so no byte is "
            f"lost and the characters may be wrong. The declared encoding of this file is "
            f"probably not the one it was written in.")
        return (bad.decode('latin1'), error.end)

    def _count(self, where, outcome):
        with self._lock:
            key = (where, outcome)
            self._counts[key] = self._counts.get(key, 0) + 1

    def _report(self, where, level, message):
        """
        Write the first few in full and count the rest.

        A column in an unexpected encoding raises one event per row, so the detailed lines are
        limited per place; the total is what `log_summary()` writes at the end.
        """
        with self._lock:
            seen = self._reported.get(where, 0)
            self._reported[where] = seen + 1
        if seen < self.detailed_reports:
            self.config_parser.print_log_message(level, message)
            if seen + 1 == self.detailed_reports:
                self.config_parser.print_log_message(
                    level,
                    f"{where}: further values of this kind are written at DEBUG only - the "
                    f"total is reported when the connection is closed.")
        else:
            self.config_parser.print_log_message('DEBUG', message)

    def summary(self):
        """`{place: {outcome: count}}` - what happened, for a report or a protocol row."""
        with self._lock:
            counts = dict(self._counts)
        collected = {}
        for (where, outcome), count in counts.items():
            collected.setdefault(where, {})[outcome] = count
        return collected

    def total(self):
        """How many values were not read by the encoding which was expected first."""
        with self._lock:
            return sum(self._counts.values())

    @staticmethod
    def is_fallback(outcome):
        """Whether an outcome is one no expected encoding could produce."""
        return not outcome.startswith('read as ') or outcome.endswith('as a last resort')

    def log_summary(self):
        """
        One line per place, written when the connection which used it is closed.

        A place where the setting had to be applied is a WARNING, because a value was changed
        or refused there. A place where only a later expected encoding was needed is an INFO:
        nothing was guessed, and the count is still the only evidence that the column is not
        in the encoding the first one assumes.
        """
        collected = self.summary()
        if not collected:
            return
        for where, outcomes in sorted(collected.items()):
            spelled = ', '.join(f'{count} {outcome}' for outcome, count in sorted(outcomes.items()))
            if any(self.is_fallback(outcome) for outcome in outcomes):
                self.config_parser.print_log_message(
                    'WARNING',
                    f"{where}: {spelled}. What did not fit the encoding(s) expected there "
                    f"({' or '.join(self.encodings)}) was handled as "
                    f"migration.on_undecodable_bytes says ('{self.policy}').")
            else:
                self.config_parser.print_log_message(
                    'INFO',
                    f"{where}: {spelled}. Nothing was guessed and no byte was lost - the "
                    f"values were read by an encoding which is expected for them, but not by "
                    f"{self.encodings[0]}, which is the one tried first.")


def handle_file_error(error):
    """
    The codec error handler `open()` is given for a file whose encoding is declared.

    It is called with one run of bytes which the declared encoding could not read, and it
    answers with what stands there instead and where to carry on. `substitute` answers with
    the characters `latin1` gives those bytes - the rest of the file stays in the encoding it
    was declared as, and no byte is lost; `remove` answers with nothing, which is what
    `errors='ignore'` did; `fail` raises what it was given, which ends the read.

    A decoder is put in `_ACTIVE` by `TextDecoder.reading()`, which `open_text()` wraps the
    read in. Without one - a handle opened by something which does not know about this - the
    default setting is applied and nothing is counted, because there is nowhere to count it.
    """
    if not isinstance(error, UnicodeDecodeError):
        ## An encoding error is the caller writing a value the target cannot hold, which is
        ## not this decision and must not be answered by one.
        raise error

    decoder = getattr(_ACTIVE, 'decoder', None)
    if decoder is None:
        return (error.object[error.start:error.end].decode('latin1'), error.end)
    return decoder.file_error(error, getattr(_ACTIVE, 'place', None))


codecs.register_error(ERROR_HANDLER, handle_file_error)
