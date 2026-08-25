# SPDX-License-Identifier: GPL-3.0-or-later
"""
The output paths of the query conversion, decided before the first file is read.

None of these answers needs a conversion: an output which would be written over its own
input, an output which exists already and may not be replaced, and two input files whose
outputs would land on the same path are all known from the names alone. They used to be
answered by the writer, which runs after a file has been converted and tested - so an output
file which existed already, on the first of twenty input files, threw the work of that file
away and stopped the run before the other nineteen were read.

Run with:  python3 -m pytest tests/test_query_output_paths.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.query_conversion.writer import OutputWriter


def writer(tmp_path, **settings):
    base = {'directory': str(tmp_path / 'out'), 'prefix': '', 'suffix': '_pg',
            'overwrite': False, 'include_original': True, 'sidecar': 'off'}
    base.update(settings)
    return OutputWriter(base, lambda level, message: None)


def input_file(tmp_path, name, text='SELECT 1;\n'):
    path = tmp_path / name
    path.write_text(text, encoding='utf-8')
    return str(path)


def test_every_path_of_the_run_is_answered_at_once(tmp_path):
    files = [input_file(tmp_path, 'a.sql'), input_file(tmp_path, 'b.sql')]
    planned = writer(tmp_path).check_all_paths(files)
    assert len(planned) == 2
    assert all(path.endswith('_pg.sql') for path in planned)


def test_an_output_which_exists_is_refused_before_anything_is_converted(tmp_path):
    files = [input_file(tmp_path, 'a.sql'), input_file(tmp_path, 'b.sql')]
    out = tmp_path / 'out'
    out.mkdir()
    (out / 'b_pg.sql').write_text('-- from an earlier run\n', encoding='utf-8')
    with pytest.raises(ValueError) as raised:
        writer(tmp_path).check_all_paths(files)
    assert 'exists already' in str(raised.value)


def test_overwrite_lets_the_same_paths_through(tmp_path):
    files = [input_file(tmp_path, 'a.sql')]
    out = tmp_path / 'out'
    out.mkdir()
    (out / 'a_pg.sql').write_text('-- from an earlier run\n', encoding='utf-8')
    assert writer(tmp_path, overwrite=True).check_all_paths(files)


def test_an_output_which_names_an_input_is_refused(tmp_path):
    files = [input_file(tmp_path, 'a.sql')]
    ## no directory of its own and no suffix: the output would be the input
    with pytest.raises(ValueError) as raised:
        writer(tmp_path, directory='', suffix='').check_all_paths(files)
    assert 'written over the file they were read from' in str(raised.value)


def test_two_inputs_whose_outputs_collide_are_refused(tmp_path):
    """
    Two files of the same name in different directories, written into one output directory,
    would land on one path - and the second answer would quietly replace the first.
    """
    first = tmp_path / 'reports'
    second = tmp_path / 'billing'
    first.mkdir()
    second.mkdir()
    files = [input_file(first, 'daily.sql'), input_file(second, 'daily.sql')]
    with pytest.raises(ValueError) as raised:
        writer(tmp_path).check_all_paths(files)
    assert 'would both be written to' in str(raised.value)


def test_the_same_name_in_two_directories_is_fine_without_a_common_output(tmp_path):
    first = tmp_path / 'reports'
    second = tmp_path / 'billing'
    first.mkdir()
    second.mkdir()
    files = [input_file(first, 'daily.sql'), input_file(second, 'daily.sql')]
    ## each output stands next to its own input
    assert len(writer(tmp_path, directory='').check_all_paths(files)) == 2


def test_nothing_is_written_by_the_check(tmp_path):
    files = [input_file(tmp_path, 'a.sql')]
    writer(tmp_path).check_all_paths(files)
    assert not os.path.isdir(str(tmp_path / 'out'))
