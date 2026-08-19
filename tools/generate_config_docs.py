#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""
Generate docs/config_reference.md from credativ_pg_migrator/config.schema.json.

The schema is the source of truth for the configuration language. This script turns it
into the human reference, so the two cannot drift. Run it after editing the schema:

    python3 tools/generate_config_docs.py

tests/test_config_docs.py fails when the checked-in Markdown differs from what this
script produces, so a schema change that is not regenerated is caught in CI.
"""

import json
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(REPO, 'credativ_pg_migrator', 'config.schema.json')
OUTPUT_PATH = os.path.join(REPO, 'docs', 'config_reference.md')

BANNER = (
    "<!-- GENERATED FILE - DO NOT EDIT.\n"
    "     Generated from credativ_pg_migrator/config.schema.json by tools/generate_config_docs.py.\n"
    "     Edit the schema and re-run the generator. -->\n"
)


def load_schema():
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def resolve(node, root):
    """Follow a local $ref, keeping any sibling keys of the reference itself."""
    if not isinstance(node, dict) or '$ref' not in node:
        return node
    target = root
    for part in node['$ref'].lstrip('#/').split('/'):
        target = target[part]
    merged = dict(target)
    for key, value in node.items():
        if key != '$ref':
            merged[key] = value
    return merged


def type_name(node, root):
    """A short human name for what a property accepts."""
    node = resolve(node, root)

    if 'x-standard-values' in node:
        return 'string'
    if 'oneOf' in node:
        return ' \\| '.join(type_name(option, root) for option in node['oneOf'])

    declared = node.get('type')
    if isinstance(declared, list):
        names = []
        for one in declared:
            if one == 'null':
                continue
            names.append(type_name(dict(node, type=one), root))
        rendered = ' \\| '.join(names) if names else 'null'
        if 'null' in declared:
            rendered += ' \\| null'
        return rendered

    if declared == 'array':
        item = resolve(node.get('items', {}), root)
        if item.get('type') == 'array':
            lo, hi = item.get('minItems'), item.get('maxItems')
            if lo and lo == hi:
                return f'list of {lo}-element lists'
            return 'list of lists'
        if item.get('type') == 'object' or 'oneOf' in item:
            return 'list of entries'
        if item.get('type'):
            return f"list of {item['type']}"
        return 'list'

    if declared == 'object':
        if 'properties' in node:
            return 'block'
        if isinstance(node.get('additionalProperties'), dict):
            return 'map'
        return 'block'

    if declared == 'string' and node.get('enum') == ['all']:
        return '"all"'

    return declared or 'any'


def allowed_values(node, root):
    node = resolve(node, root)
    # A setting with standard values and aliases shows only the standard ones here; the
    # aliases are listed separately in the notes, so the two are never confused.
    if 'x-standard-values' in node:
        return ', '.join(f'`{v}`' for v in node['x-standard-values'])
    if 'oneOf' in node:
        parts = [allowed_values(option, root) for option in node['oneOf']]
        return ' \\| '.join(p for p in parts if p)
    if 'enum' in node:
        return ', '.join(f'`{v}`' for v in node['enum'])
    item = resolve(node.get('items', {}), root) if node.get('type') == 'array' else {}
    if 'enum' in item:
        return 'items: ' + ', '.join(f'`{v}`' for v in item['enum'])
    bounds = []
    if 'minimum' in node:
        bounds.append(f">= {node['minimum']}")
    if 'maximum' in node:
        bounds.append(f"<= {node['maximum']}")
    return ', '.join(bounds)


def default_value(node, root):
    node = resolve(node, root)
    if 'default' not in node:
        return ''
    value = node['default']
    if value is None:
        return '`null`'
    if isinstance(value, bool):
        return f'`{str(value).lower()}`'
    if isinstance(value, (list, dict)):
        return f'`{json.dumps(value)}`'
    return f'`{value}`'


def notes(node, root, name, required):
    node = resolve(node, root)
    out = []
    if required:
        out.append('**required**')
    if node.get('deprecated'):
        out.append('**deprecated**')
    if node.get('x-implemented') is False:
        out.append('**not implemented**')
    engines = node.get('x-applies-to-engines')
    if engines:
        out.append('only for ' + ', '.join(f'`{e}`' for e in engines))
    superseded = node.get('x-superseded-by')
    if superseded:
        out.append(f'use `{superseded}` instead')
    prefix = '. '.join(out)
    description = (node.get('description') or '').strip()
    tail = [description] if description else []
    for extra_key in ('x-arity-note', 'x-required-reason'):
        if node.get(extra_key):
            tail.append(f'({node[extra_key]})')
    aliases = node.get('x-aliases')
    if aliases:
        grouped = {}
        for alias, standard in aliases.items():
            grouped.setdefault(standard, []).append(alias)
        spelled = '; '.join(
            f"{', '.join(f'`{a}`' for a in sorted(names))} = `{standard}`"
            for standard, names in sorted(grouped.items()))
        tail.append(f'Accepted aliases: {spelled}.')

    rendered = ' '.join(tail)
    if prefix and rendered:
        rendered = f'{prefix}. {rendered}'
    elif prefix:
        rendered = prefix + '.'
    return rendered.replace('\n', ' ')


def heading_text(path, node, root):
    """The exact heading a block gets, so links and headings are built from one place."""
    resolved = resolve(node, root)
    suffix = '[]' if resolved.get('type') == 'array' else ''
    return f'`{path}{suffix}`'


def anchor(heading_text):
    """
    The fragment GitHub derives from a heading: lower-cased, backticks and other
    punctuation dropped, spaces turned into hyphens. Underscores are kept - replacing
    them would produce links that do not resolve.
    """
    text = heading_text.strip().lower()
    text = re.sub(r'[^\w\s-]', '', text)
    return re.sub(r'\s+', '-', text)


def is_block(node, root):
    node = resolve(node, root)
    if node.get('type') == 'object' and node.get('properties'):
        return True
    if node.get('type') == 'array':
        item = resolve(node.get('items', {}), root)
        return bool(item.get('type') == 'object' and item.get('properties'))
    return False


def child_of(node, root):
    """The object carrying the properties: the node itself, or an array's item."""
    node = resolve(node, root)
    if node.get('type') == 'array':
        return resolve(node.get('items', {}), root)
    return node


def render_table(node, root, path, lines):
    holder = child_of(node, root)
    props = holder.get('properties', {})
    required = set(holder.get('required', []))
    if not props:
        return []

    lines.append('| Key | Type | Allowed values | Default | Notes |')
    lines.append('|---|---|---|---|---|')
    nested = []
    for name, child in props.items():
        resolved = resolve(child, root)
        key_cell = f'`{name}`'
        if is_block(resolved, root):
            sub_path = f'{path}.{name}' if path else name
            key_cell = f'[`{name}`](#{anchor(heading_text(sub_path, child, root))})'
            nested.append((sub_path, child))
        lines.append(
            f'| {key_cell} '
            f'| {type_name(child, root)} '
            f'| {allowed_values(child, root)} '
            f'| {default_value(child, root)} '
            f'| {notes(child, root, name, name in required)} |'
        )
    lines.append('')
    return nested


def render_block(path, node, root, lines, level):
    resolved = resolve(node, root)
    holder = child_of(resolved, root)

    lines.append(f'{"#" * level} {heading_text(path, node, root)}')
    lines.append('')
    description = (resolved.get('description') or holder.get('description') or '').strip()
    if description:
        lines.append(description)
        lines.append('')
    for flag, text in (
        ('x-implemented', '> **Not implemented.** The block is read but has no effect yet.'),
        ('deprecated', '> **Deprecated.**'),
    ):
        if flag == 'x-implemented' and resolved.get(flag) is False:
            lines.append(text)
            lines.append('')
        elif flag == 'deprecated' and resolved.get(flag):
            lines.append(text)
            lines.append('')
    if isinstance(holder.get('additionalProperties'), dict) and not holder.get('properties'):
        inner = holder['additionalProperties']
        lines.append(f'Free-form map. Each value is: {type_name(inner, root)}.')
        inner_desc = (inner.get('description') or '').strip()
        if inner_desc:
            lines.append('')
            lines.append(inner_desc)
        lines.append('')
        if is_block(inner, root):
            render_block(f'{path}.<name>', inner, root, lines, min(level + 1, 6))
        return

    for sub_path, child in render_table(resolved, root, path, lines):
        render_block(sub_path, child, root, lines, min(level + 1, 6))


def build(root):
    lines = [BANNER]
    lines.append('# credativ-pg-migrator - configuration reference')
    lines.append('')
    lines.append(
        'Every option the migrator understands, with its type, its allowed values, its default '
        'and where it applies. This file is generated from `credativ_pg_migrator/config.schema.json`, which the '
        'migrator also validates your configuration against at startup - so what is written here '
        'is what the code reads.'
    )
    lines.append('')
    lines.append('**Looking for a file to start from?** Copy the example matching your source '
                 'database from [`docs/configs/`](configs/) - those are complete, valid, runnable '
                 'configurations. This reference is for looking options up, not for copying whole.')
    lines.append('')
    lines.append('## How to read the tables')
    lines.append('')
    lines.append('- **Type** `block` is a nested mapping and has its own section; `list of entries` '
                 'is a list of such mappings. A type written `a \\| b` accepts either form.')
    lines.append('- **Default** is what the migrator uses when the key is absent. An empty cell '
                 'means the option has no default and is simply not applied.')
    lines.append('- **Notes** carries `required`, `deprecated`, `not implemented`, and the source '
                 'engines an option applies to. An option with no engine listed applies to all of them.')
    lines.append('- Keys marked **required** must be present; the migrator stops without them.')
    lines.append('')

    top_required = set(root.get('required', []))
    if top_required:
        lines.append('## Required keys')
        lines.append('')
        lines.append('The following top-level keys must be present: '
                     + ', '.join(f'`{k}`' for k in sorted(top_required)) + '.')
        note = root.get('x-required-note')
        if note:
            lines.append('')
            lines.append(note)
        lines.append('')

    lines.append('## Top-level keys')
    lines.append('')
    nested = render_table(root, root, '', lines)

    lines.append('---')
    lines.append('')
    for path, node in nested:
        render_block(path, node, root, lines, 2)
        lines.append('---')
        lines.append('')

    while lines and lines[-1] in ('', '---'):
        lines.pop()
    return '\n'.join(lines) + '\n'


def main():
    root = load_schema()
    rendered = build(root)
    if '--check' in sys.argv:
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as handle:
            current = handle.read()
        if current != rendered:
            print('docs/config_reference.md is out of date - run tools/generate_config_docs.py',
                  file=sys.stderr)
            return 1
        print('docs/config_reference.md is up to date')
        return 0
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as handle:
        handle.write(rendered)
    print(f'wrote {OUTPUT_PATH} ({len(rendered.splitlines())} lines)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
