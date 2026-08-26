import re
import os
import glob
from typing import List, Dict, Any
from credativ_pg_migrator import identifier_case

class SourceLine:
    def __init__(self, line_number: int, content: str):
        self.line_number = line_number
        self.content = content

    def __repr__(self):
        return f"Line {self.line_number}: {self.content}"

class OutputLine:
    def __init__(self, new_line_number: int, source_array: str, original_line_number: int, content: str):
        self.new_line_number = new_line_number
        self.source_array = source_array
        self.original_line_number = original_line_number
        self.content = content
        self.if_command_level = 0 # Special item for Pass 12

    def __repr__(self):
        return f"{self.new_line_number} [{self.source_array}:{self.original_line_number}] L{self.if_command_level}: {self.content}"


class TsqlParser:
    def __init__(self, code_str: str, config_parser=None, implicit_return=False, view_converter=None, settings=None, functions_mapping_converter=None, pseudo_table_converter=None):
        self.code_str = code_str
        self.config_parser = config_parser
        self.implicit_return = implicit_return
        self.view_converter = view_converter
        self.settings = settings
        self.functions_mapping_converter = functions_mapping_converter
        self.pseudo_table_converter = pseudo_table_converter
        self.raw_lines = []
        self.body_lines = []
        self.header_lines = []
        self.comments = []
        self.variables = []
        self.inserts = []
        self.update_commands = []
        self.select_commands = []
        self.exec_commands = []
        self.if_commands = []
        self.delete_commands = []
        self.print_commands = []
        self.set_commands = []
        self.raiserror_commands = []
        self.cursors = []
        self.cursor_commands = []
        self.while_commands = []

    def log(self, message):
        if self.config_parser:
            self.config_parser.print_log_message('DEBUG', 'TsqlParser: ' + message)
        else:
            print(f'[LOG] {message}')

    def extract_implicit_return_schema(self) -> List[Dict]:
        """
        Parses the code to find the implicit return SELECT statement,
        then uses SQLGlot to extract column names and infer basic types.
        """
        self.read_code()
        self.parse_header_and_body_boundary()
        self.pass_1_split_inline_comments()

        # Pass 0b
        self.pass_0b_map_tempdb()

        # Pass 1c
        self.pass_1c_split_inline_goto()
        self.pass_2_extract_comments()
        self.pass_3_parse_variables()

        # Pass 3c
        self.pass_3c_parse_cursors()
        self.pass_3b_split_inline_ifs()

        # Pass 3d
        self.pass_7d_parse_goto_and_labels()
        self.pass_4_parse_inserts()

        # Pass 4b
        self.pass_4b_parse_cursor_commands()

        # Pass 4c
        self.pass_4c_parse_create_drop_table()
        self.pass_5_parse_updates()
        self.pass_5b_parse_deletes()
        self.pass_5c_parse_prints()
        self.pass_5d_parse_sets()
        self.pass_5e_parse_raiserror()
        self.pass_6_parse_selects()

        for cmd_obj in self.select_commands:
            content = cmd_obj['content']
            normalized = re.sub(r'\s+', ' ', content)
            is_assignment = bool(re.match(r'^SELECT\s+(@[\w@]+|locvar_[\w]+)\s*(:=|=)', normalized, re.IGNORECASE))
            has_into = bool(re.search(r'\bINTO\b', normalized, re.IGNORECASE))

            if not is_assignment and not has_into:
                # Parse with SQLGlot
                import sqlglot
                from sqlglot import exp
                try:
                    parsed = sqlglot.parse_one(content, read='tsql')
                    
                    node = parsed
                    while isinstance(node, exp.Union):
                        node = node.this
                        
                    expressions = []
                    if isinstance(node, exp.Select):
                        expressions = node.expressions

                    if expressions:
                        schema = []
                        name_counts = {}
                        for idx, p in enumerate(expressions):
                            alias = ""
                            expr = p
                            if isinstance(p, exp.Alias):
                                alias = p.alias
                                expr = p.this
                            elif isinstance(p, exp.Column):
                                alias = p.name
                            
                            inferred_type = "varchar"
                            if isinstance(expr, (exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min)):
                                inferred_type = "numeric"
                            elif isinstance(expr, exp.Literal):
                                if expr.is_int: inferred_type = "integer"
                                elif expr.is_number: inferred_type = "numeric"
                            
                            if not alias or alias == 'unknown_col':
                                alias = f"col{idx}"
                            
                            if alias.lower() in name_counts:
                                name_counts[alias.lower()] += 1
                                alias = f"{alias}_{name_counts[alias.lower()]}"
                            else:
                                name_counts[alias.lower()] = 0
                                
                            schema.append({'name': alias, 'system_type_name': inferred_type})
                        return schema
                except Exception as e:
                    self.log(f"SQLGlot parsing failed for implicit return schema: {e}")
                    
        return []

    def read_code(self):
        lines = self.code_str.splitlines()

        for idx, line in enumerate(lines):
            # "At the beginning whole source code of an object must be read and divided by lines and these stored in an array together with line numbers"
            # "Trailing spaces must be removed from each line"
            clean_content = line.rstrip()
            self.raw_lines.append(SourceLine(idx + 1, clean_content))

    def mask_comments_and_literals(self, content: str, in_block_comment):
        """
        The line with its comments and string literals replaced by spaces, and the state of the
        block comment at the end of it. The result has the length of the original, so a position
        found in it addresses the same character of the original line.

        A block comment may contain another one, in Transact-SQL as in PostgreSQL, so the state
        carried from line to line is how many of them are open. It used to be the answer to
        whether one was open at all, and the first '*/' of a comment framed by rows of dashes
        ended it while the comment went on.
        """
        comment_depth = int(in_block_comment)
        masked = list(content)
        index = 0
        length = len(content)
        while index < length:
            if comment_depth > 0:
                if content.startswith('*/', index):
                    masked[index] = masked[index + 1] = ' '
                    index += 2
                    comment_depth -= 1
                elif content.startswith('/*', index):
                    masked[index] = masked[index + 1] = ' '
                    index += 2
                    comment_depth += 1
                else:
                    masked[index] = ' '
                    index += 1
                continue
            if content.startswith('/*', index):
                masked[index] = masked[index + 1] = ' '
                index += 2
                comment_depth += 1
                continue
            if content.startswith('--', index):
                for position in range(index, length):
                    masked[position] = ' '
                break
            if content[index] == "'":
                masked[index] = ' '
                index += 1
                while index < length:
                    if content[index] == "'":
                        masked[index] = ' '
                        index += 1
                        if index < length and content[index] == "'":
                            masked[index] = ' '
                            index += 1
                            continue
                        break
                    masked[index] = ' '
                    index += 1
                continue
            index += 1
        return ''.join(masked), comment_depth

    def find_header_end(self):
        """
        The index of the line carrying the AS which ends the header of the routine, and the
        position of that AS in the line.

        The AS is searched in the code alone: a comment in front of the routine regularly
        contains the word ("Created as a stored procedure to allow for re-usage"), and taking
        that one left the CREATE line and the parameters of the routine in the body, where every
        one of them was reported as a line which could not be processed. The search starts at the
        CREATE of the routine for the same reason. A source without a CREATE - a fragment - is
        answered with the first AS of the text, as before.
        """
        in_block_comment = False
        create_seen = False
        for i, line in enumerate(self.raw_lines):
            masked, in_block_comment = self.mask_comments_and_literals(line.content, in_block_comment)
            if not create_seen:
                if not re.search(r'(?i)\bCREATE\b', masked):
                    continue
                create_seen = True
            match = re.search(r'\bAS\b', masked, re.IGNORECASE)
            if match:
                return i, match.end()

        if not create_seen:
            for i, line in enumerate(self.raw_lines):
                match = re.search(r'\bAS\b', line.content, re.IGNORECASE)
                if match:
                    return i, match.end()
        return -1, -1

    def parse_header_and_body_boundary(self):
        """
        Identify where the header ends and the body begins.
        Header ends at 'AS'. Body starts after 'AS'.
        Body ends at 'END'.
        """
        # Parsing of header
        # Header starts with "CREATE PROCEDURE" or "CREATE FUNCTION" or "CREATE TRIGGER" ... ends with "AS" key word

        end_index = -1

        # Determine body start (after 'AS') - the AS of the code, not one inside a comment
        as_index, after_as_position = self.find_header_end()

        if as_index != -1:
            line = self.raw_lines[as_index]
            # Check if there's anything after 'AS'
            after_as = line.content[after_as_position:]
            if after_as.strip():
                # Split the line into two SourceLines
                header_part = line.content[:after_as_position]
                body_part = after_as

                # Update current line to be just the header part
                self.raw_lines[as_index].content = header_part

                # Insert the rest as the next line (preserve line_number for tracking)
                self.raw_lines.insert(as_index + 1, SourceLine(line.line_number, body_part))

        if as_index != -1:
            # Header lines: 0 to as_index (inclusive)
            # Rule: "remove all spaces at the beginning and at the end of each line"
            raw_header = self.raw_lines[:as_index+1]
            self.header_lines = [SourceLine(l.line_number, l.content.strip()) for l in raw_header]

            # Body is everything after 'AS'
            raw_body = self.raw_lines[as_index+1:]

            # Rule: "remove all spaces at the beginning and at the end of each line"
            self.body_lines = [SourceLine(l.line_number, l.content.strip()) for l in raw_body]

        else:
            self.log("Error: No AS keyword found in header.")

    def is_encapsulated(self, content: str, index: int) -> bool:
        """
        Check if the character at 'index' is inside quotes.
        """
        in_single_quote = False
        in_double_quote = False

        for i in range(index + 1): # Iterate up to and including the index?
            # actually we just need state at index.
            # But the state changes at the quote character itself.
            # If line is: "string" -- index of " is 0.
            # if i==0: content[i] is ".
            # If we are verifying if content[index] is inside, we check if we occupied a state *before* it?
            # Or is content[index] the marker itself?
            pass

        # Simplified: scan string from start.
        for i, char in enumerate(content):
            if i == index:
                return in_single_quote or in_double_quote

            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

        return False

    def split_outside_parens(self, statement: str, separator: str = ',') -> List[str]:
        """
        The parts of a statement separated by the given character, counting only the occurrences
        of it which stand outside parentheses, string literals and comments.
        """
        masked, _ = self.mask_comments_and_literals(statement, False)
        parts = []
        paren_level = 0
        part_start = 0
        for position, char in enumerate(masked):
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            elif char == separator and paren_level == 0:
                parts.append(statement[part_start:position])
                part_start = position + 1
        parts.append(statement[part_start:])
        return parts

    def replace_commas_outside_parens(self, s, stop_word=None):
        result = []
        paren_level = 0
        in_single_quote = False
        in_double_quote = False
        
        stop_word_lower = stop_word.lower() if stop_word else None
        stopped = False

        i = 0
        while i < len(s):
            char = s[i]
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == '(' and not in_single_quote and not in_double_quote:
                paren_level += 1
            elif char == ')' and not in_single_quote and not in_double_quote:
                paren_level -= 1
            
            if stop_word_lower and not stopped and paren_level == 0 and not in_single_quote and not in_double_quote:
                if s[i:].lower().startswith(stop_word_lower):
                    end_idx = i + len(stop_word_lower)
                    if (i == 0 or not s[i-1].isalnum() and s[i-1] != '_') and \
                       (end_idx == len(s) or not s[end_idx].isalnum() and s[end_idx] != '_'):
                        stopped = True
            
            if char == ',' and paren_level == 0 and not in_single_quote and not in_double_quote and not stopped:
                result.append(';')
            else:
                result.append(char)
            
            i += 1
        return "".join(result)

    def normalize_delete_with_second_from(self, statement: str) -> str:
        """
        'DELETE FROM t FROM t, x WHERE ...' of T-SQL as 'DELETE FROM t USING x WHERE ...'.

        T-SQL names the table to delete from first and lists the tables the condition reads in a
        second FROM clause. PostgreSQL has USING for that list and refuses the second FROM with
        'syntax error at or near "from"'; the table itself must not appear in the list either,
        there it would be a second, unrelated instance of it and the statement would delete
        every row. A list containing anything but plain table names is left alone.
        """
        match = re.match(r'(?is)^\s*DELETE\s+FROM\s+([^\s,()]+)\s+FROM\s+(.*?)(\s+WHERE\b.*)?$', statement)
        if not match:
            return statement
        target, sources, where = match.group(1), match.group(2), match.group(3) or ''
        if '(' in sources or ' select ' in f' {sources.lower()} ':
            return statement

        def bare_name(source):
            return source.split()[0].strip('"[]').lower() if source.split() else ''

        other_sources = [source.strip() for source in sources.split(',') if source.strip()]
        other_sources = [source for source in other_sources if bare_name(source) != target.strip('"[]').lower()]

        if other_sources:
            return f"DELETE FROM {target} USING {', '.join(other_sources)}{where}"
        return f"DELETE FROM {target}{where}"

    def inside_open_case(self, collected_lines: List[str]) -> bool:
        """
        True when the statement collected so far has a CASE expression which is not closed yet.

        The WHEN, ELSE and END of a CASE belong to the expression, but the same words end a
        block of the routine, so a statement was cut at the first line of a CASE written over
        several lines - the rest of it ('END', 'FROM ...', 'WHERE ...') was left behind as
        separate lines which no pass could parse. String literals are left out of the count,
        'END' inside one is not the end of a CASE.
        """
        if not collected_lines:
            return False
        text = " ".join(collected_lines)
        text = re.sub(r"'(?:[^']|'')*'", "''", text)
        opened = len(re.findall(r'(?i)\bCASE\b', text))
        closed = len(re.findall(r'(?i)\bEND\b', text))
        return opened > closed

    def find_unquoted_marker(self, content: str, markers: List[str]) -> tuple:
        """
        Finds the first occurrence of any marker in 'markers' that is NOT encapsulated.
        Returns (index, marker_found) or (-1, None).
        """
        in_single_quote = False
        in_double_quote = False

        # We need to iterate char by char to track quotes
        # But also check for markers at each position

        i = 0
        while i < len(content):
            char = content[i]

            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            if not in_single_quote and not in_double_quote:
                for marker in markers:
                    if content.startswith(marker, i):
                        return i, marker

            i += 1

        return -1, None

    def pass_1_split_inline_comments(self):
        """
        Pass 1: Splits lines containing inline comments (--, /*, */) into two lines.
        Prioritizes splitting.
        """
        self.log("Running Pass 1: Split Inline Comments")

        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content

            # search for lines which do not start with "--" or "/*" or "*/" (implicit?)
            # Rule: "searches for lines which do not start with "--" or "/*""
            # If it starts with it, we skip splitting.

            # Wait, what if it starts with "*/"?
            # Rule 33: "if line contains "*/" ... line must be divided ... part with "*/" is kept as original"
            # It doesn't say "does not start with".
            # Rule 20: "searches for lines which do not start with "--" or "/*""

            starts_with_comment = content.strip().startswith("--") or content.strip().startswith("/*")
            # Note: "start with" usually means from index 0 or ignoring whitespace?
            # "Trailing spaces must be removed" (Line 8). Leading spaces?
            # Rule 7: "Source code ... divided by lines ... Trailing spaces must be removed".
            # Usually strict "starts with" implies index 0.
            # But in code
            #    -- comment
            # starts with space.
            # Let's assume strict startsswith for now or stripped?
            # "searches for lines which do not start with"
            # Given SQL, probably strip() is safer, but strictly the rule might mean index 0.
            # I will use strict index 0 based on "Trailing spaces must be removed" but nothing about leading.

            # Markers to check: --, /*, */
            # Logic varies per marker?
            # Rule 23 (--): "part before '--' is kept as original... part starting with '--' is kept as new"
            # Rule 28 (/*): "part before '/*' is kept as original... part starting with '/*' is kept as new"
            # Rule 33 (*/): "part with '*/' is kept as original... part after '*/' is kept as new" -> WAIT.
            # Rule 37: "the part after '*/' is kept as the new line"

            # We need to find the FIRST occurrence of ANY of these valid markers?
            # Or is there a priority?
            # Usually check left-to-right.

            idx, marker = self.find_unquoted_marker(content, ["--", "/*", "*/"])

            if idx != -1:
                # Check exclusion: specific start conditions
                # if marker == "--" or marker == "/*": check if line starts with it

                # If idx == 0, it starts with it.
                # If idx > 0 but only whitespace before?
                # Rule says "lines which do not start with".
                # If I have "   -- comment", does it "start with --"?
                # If I don't split it, it remains "   -- comment".
                # Pass 1 says "Comments start ... with '--'".
                # If I treat "   -- " as a comment line, it works for Pass 1.
                # So "starts with" probably allows leading whitespace or implies checking if the comment is the *primary* thing on the line?
                # Actually, if idx > 0, we split.
                # "part before" -> "   " (kept as original).
                # New line -> "-- comment".
                # Original line "   " is effectively empty/useless but keeps line number.
                # This seems safe.
                # Exception: Rule 20 explicitly says "searches for lines which DO NOT START WITH ... or ...".
                # If it starts with it, we DO NOT split.

                if (marker == "--" or marker == "/*") and idx == 0:
                     # Starts with it (at index 0). No split needed.
                     i += 1
                     continue

                # Rule 33 for */: "divided into two parts - part with */ and the rest"
                # "part with */ is kept as original"
                # "part after */ is kept as new line"
                # Example: " code */ more code "
                # Original: " code */"
                # New: " more code "

                # NOTE: the split itself is done further below, after the checks for a line
                # which starts with a comment marker. It used to be done here as well, with the
                # same content and without ending the iteration, so every line carrying an
                # inline comment was split twice and the comment ended up in the output twice
                # ("/*-- instance_id*/" once per split).

                # We stay at 'i' ? or move to 'i+1'?
                # If we split:
                # L1: "select 1 -- comm"
                # Becomes:
                # L1: "select 1 "
                # L2: "-- comm"
                # L3...
                # We processed L1. Now L2 is "-- comm".
                # Next iter should check L2. L2 starts with "--", so won't split.
                # If L1 was "select 1 /* ... */ select 2"
                # Split at /*
                # L1: "select 1 "
                # L2: "/* ... */ select 2"
                # Next loop checks L2. Starts with /*. Won't split?
                # WAIT. If L2 is "/* c */ select 2", it starts with /*.
                # But it contains */ which is a split marker!
                # Does logic allow finding */ if it starts with /*?
                # Rule 20: "searches for lines which do not start with -- or /*".
                # If it starts with /*, we skip split check?
                # Then we miss the */ split?
                # The rule specifically references finding "lines which do not start with...".
                # It does NOT explicitly say "And for lines which DO start with ..., check for */".
                # However, Rule 33 (for */) is separate bullet.
                # It does NOT say "searches for lines which do not start with...".
                # It just says "if line contains */ ...".
                # So we CAN split on */ even if it starts with /*.

                # Rule 20: "searches for lines which do not start with -- or /*"
                if (content.startswith("--") and idx == 0) or (content.startswith("/*") and idx == 0):
                    # Starts with it (at index 0). No split needed, UNLESS it contains */ (Rule 33)
                    # Rule 33: "if line contains */ ... then this line must be divided"
                    # But Rule 41 implies we don't parse inside comments?
                    # Wait, Pass 1 is "update of inline comments".
                    # Review Rule 33. "if line contains */ ... divided"

                    # If line starts with /* and contains */
                    # e.g. "/* c */ select 1"
                    # Split at */.
                    # Part 1: "/* c */"
                    # Part 2: " select 1"

                    if "*/" in content:
                        idx_end, marker_end = self.find_unquoted_marker(content, ["*/"])
                        if idx_end != -1:
                            # We found */. It is a split point.
                            marker = marker_end
                            idx = idx_end
                        else:
                            # Starts with comment, no */ found (or quoted).
                            i += 1
                            continue
                    else:
                        # Starts with comment, no */.
                        i += 1
                        continue

                is_start_comment = (content.startswith("--") or content.startswith("/*"))
                if is_start_comment:
                    # Only check for */
                    if "*/" in content:
                        idx_end, marker_end = self.find_unquoted_marker(content, ["*/"])
                        if idx_end != -1:
                            # Verify we aren't splitting the same marker we started with?
                            # If content is "/* */", starts with /*.
                            # find */ -> finds index 2.
                            # Split after */.
                            # part1: "/* */"
                            # part2: ""
                            # Seems valid.
                            marker = marker_end
                            idx = idx_end
                        else:
                            i += 1
                            continue
                    else:
                        i += 1
                        continue

                # Performed split logic above...
                # Re-evaluating split logic for */

                if marker == "*/":
                    # Split AFTER
                    split_point = idx + 2
                    part1 = content[:split_point]
                    part2 = content[split_point:]

                    self.body_lines[i].content = part1
                    new_line = SourceLine(self.body_lines[i].line_number + 1, part2)
                    self.body_lines.insert(i + 1, new_line)

                    for j in range(i + 2, len(self.body_lines)):
                        self.body_lines[j].line_number += 1

                    # Increment i to process the NEW line next?
                    # Or re-process current line?
                    # If we split "select 1 /* comm */ select 2"
                    # idx of /* is > 0.
                    # Split 1 (at /*):
                    # L1: "select 1 "
                    # L2: "/* comm */ select 2"
                    # Next iter: checks L2.
                    # L2 starts with /*.
                    # Finds */.
                    # Split 2 (at */):
                    # L2: "/* comm */"
                    # L3: " select 2"
                    # Seems correct.
                    # So we just increment i?
                    # But what if L1 had "/* */ /* */"?
                    # "select /* 1 */ /* 2 */"
                    # Split at first /*.
                    # L1: "select "
                    # L2: "/* 1 */ /* 2 */"
                    # Next iter I check L2.
                    # L2 starts with /*.
                    # Finds */.
                    # Split at first */.
                    # L2: "/* 1 */"
                    # L3: " /* 2 */"
                    # Next iter check L3.
                    # L3 starts with " " (space)? No, check startswith strictly?
                    # If L3 is " /* 2 */", it starts with space. So not startswith "/*".
                    # Finds /*.
                    # Split at /*.
                    # L3: " "
                    # L4: "/* 2 */"
                    # Seems robust.

                    # One Edge Case: "select 1 -- comm1 -- comm2"
                    # Split at first --.
                    # L1: "select 1 "
                    # L2: "-- comm1 -- comm2"
                    # Check L2. Starts with --. Skip.
                    # Result L2 includes both. Correct (it's a comment).

                else:
                    # Marker is -- or /* and NOT at start
                    split_point = idx
                    part1 = content[:split_point]
                    part2 = content[split_point:]

                    self.body_lines[i].content = part1
                    new_line = SourceLine(self.body_lines[i].line_number + 1, part2)
                    self.body_lines.insert(i + 1, new_line)

                    for j in range(i + 2, len(self.body_lines)):
                        self.body_lines[j].line_number += 1

                # IMPORTANT: Since we inserted a line at i+1, and we want to process that line next,
                # we just increment i (which points to i+1).
                # But wait, did we finish processing part1?
                # If L1 was "select 1 -- c1 -- c2", part1 is "select 1 ".
                # Are there more markers in part1?
                # "select 1 /* */ -- "
                # find_unquoted_marker returns FIRST.
                # So part1 is clean up to the split point.
                # So we can move to next line.
                i += 1

            else:
                # No marker found
                i += 1



    def pass_0b_map_tempdb(self):
        self.log("Running Pass 0b: Map tempdb to pg_temp")
        for line in self.body_lines:
            import re
            line.content = re.sub(r'tempdb\.\.', 'pg_temp.', line.content, flags=re.IGNORECASE)

    def pass_1c_split_inline_goto(self):
        self.log("Running Pass 1c: Split Inline GOTO")
        new_body_lines = []
        for line in self.body_lines:
            content = line.content.strip()
            if re.match(r'^GOTO\b', content, re.IGNORECASE):
                new_body_lines.append(line)
                continue
                
            m = re.search(r'\s+(GOTO\s+[a-zA-Z0-9_]+)', content, re.IGNORECASE)
            if m:
                in_single = False
                in_double = False
                split_idx = -1
                for j in range(len(content) - 4):
                    if content[j] == "'": in_single = not in_single
                    elif content[j] == '"': in_double = not in_double
                    if not in_single and not in_double:
                        if content[j:j+5].upper() == ' GOTO' and (j+5 == len(content) or content[j+5].isspace()):
                            split_idx = j
                            break
                if split_idx != -1:
                    part1 = content[:split_idx].strip()
                    part2 = content[split_idx:].strip()
                    print(f"Split part 1: {part1}")
                    new_body_lines.append(type(line)(line.line_number, part1))
                    print(f"Split part 2: {part2}")
                    new_body_lines.append(type(line)(line.line_number + 0.1, part2))
                else:
                    new_body_lines.append(line)
            else:
                new_body_lines.append(line)
        self.body_lines = new_body_lines

    def pass_2_extract_comments(self):
        """
        Pass 2: Extracts comments from body lines.
        Removes comment lines from body lines.
        Stores comments in self.comments.
        """
        self.log("Running Pass 2: Extract Comments")

        new_body_lines = []
        comment_depth = 0
        current_comment_lines = []
        current_comment_start_line = -1

        def close_comment():
            # Rule: "remove all spaces ... keep new line characters"
            self.comments.append({
                "line": current_comment_start_line,
                "content": "\n".join(l.strip() for l in current_comment_lines)
            })

        for line in self.body_lines:
            content = line.content.strip()

            if comment_depth > 0:
                current_comment_lines.append(line.content)
                ## A comment may contain another one, and the comment ends where as many '*/'
                ## have been read as there were '/*'. Ending it at the first '*/' tore a comment
                ## framed by rows of dashes apart - what followed the row was read as code and
                ## the '*/' of the comment itself was left over as a statement of its own.
                comment_depth += content.count('/*') - content.count('*/')
                if comment_depth <= 0:
                    comment_depth = 0
                    close_comment()
                    current_comment_lines = []
                continue

            # Not in comment block
            if content.startswith("/*"):
                comment_depth = content.count('/*') - content.count('*/')
                current_comment_start_line = line.line_number
                current_comment_lines.append(line.content)

                if comment_depth <= 0:
                    # Ends on same line
                    comment_depth = 0
                    close_comment()
                    current_comment_lines = []
                continue

            if content.startswith("--"):
                # Single line comment
                # Rule 47: "All comments starting with '--' must be encapsulated into '/*' and '*/'"
                ## The text of the comment is not SQL, so a '/*' or a '*/' standing in it is taken
                ## apart: either of them would open or close a comment which the text does not
                ## mean, and the '*/' of '-- see the note */' closed the comment written around it
                ## and left the rest of the line behind as code.
                raw_text = line.content.replace('/*', '/ *').replace('*/', '* /')

                self.comments.append({
                    "line": line.line_number,
                    "content": "/*" + raw_text + "*/"
                })
                continue

            # If not a comment, keep the line
            new_body_lines.append(line)

        ## A comment which never ends takes the rest of the routine with it - the lines are kept
        ## as the comment they belong to and the comment is closed, so that nothing is lost.
        if current_comment_lines:
            self.log("Pass 2: a block comment was not closed - it is closed at the end of the routine")
            current_comment_lines.append("*/")
            close_comment()

        self.body_lines = new_body_lines

    def pass_3_parse_variables(self):
        """
        Parses variable declarations.
        Starts with DECLARE.
        Ends if line does not end with comma.
        Removes lines from body.
        Stores in self.variables.
        """
        self.log("Running Pass 3: Parse Variables")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            # Check for DECLARE
            if re.match(r'^DECLARE\b', content, re.IGNORECASE):
                # Distinguish between variable declaration and cursor declaration.
                # Variable declarations in Sybase must start with @ after DECLARE.
                is_cursor = False
                after_declare = re.sub(r'^DECLARE\b', '', content, flags=re.IGNORECASE).strip()
                if after_declare:
                    if not after_declare.startswith('@'):
                        is_cursor = True
                else:
                    j = i + 1
                    while j < len(self.body_lines):
                        next_content = self.body_lines[j].content.strip()
                        if next_content:
                            if not next_content.startswith('@'):
                                is_cursor = True
                            break
                        j += 1
                        
                if is_cursor:
                    new_body_lines.append(line)
                    i += 1
                    continue

                # Start of declaration
                start_line = line.line_number
                decl_lines = []

                # Consume lines until end condition met
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(decl_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)
                        if is_terminator:
                            break

                    decl_lines.append(current_line.content)

                    if current_content.lower() == "declare":
                        i += 1
                        continue

                    i += 1

                # Rule: "remove all spaces ... keep new line characters"
                # "remove all DECLARE key words ... replace ',' characters at the end of lines with semicolon ';'"

                cleaned_lines = [l.strip() for l in decl_lines]
                full_decl = "\n".join(cleaned_lines)

                # Remove leading DECLARE (case insensitive)
                full_decl = re.sub(r'^DECLARE\s+', '', full_decl, flags=re.IGNORECASE)

                # Replace commas separating variable declarations with semicolons, ignoring commas inside parens (e.g. for NUMERIC(10,2))
                full_decl = self.replace_commas_outside_parens(full_decl)

                if not full_decl.strip().endswith(';'):
                    full_decl = full_decl.rstrip() + ';'

                self.variables.append({
                    "line": start_line,
                    "content": full_decl
                })
                # Loop continues at new 'i'
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines


    def cursor_query_is_incomplete(self, cursor_lines):
        """
        Whether the declaration of a cursor read so far still waits for (a part of) its
        query: the FOR is not there yet, nothing follows it, or the query ends with a word
        which cannot be the end of it (UNION, a comma, an open parenthesis).
        """
        collected = " ".join(line.strip() for line in cursor_lines).strip()
        if not collected:
            return True
        masked, _ = self.mask_comments_and_literals(collected, False)
        for_positions = [match.end() for match in re.finditer(r'(?i)\bFOR\b', masked)]
        if not for_positions:
            return True
        if not collected[for_positions[0]:].strip():
            return True
        return bool(re.search(r'(?i)(\b(UNION(\s+ALL)?|INTERSECT|EXCEPT|AND|OR|FROM|WHERE|BY)|,|\()\s*$', collected))

    def pass_3c_parse_cursors(self):
        self.log("Running Pass 3c: Parse Cursors")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            line_content = line.content.strip()

            if re.match(r'^DECLARE\b', line_content, re.IGNORECASE):
                if re.search(r'\bCURSOR\b', line_content, re.IGNORECASE):
                    start_line = line.line_number
                    cursor_lines = []
                    while i < len(self.body_lines):
                        current_line = self.body_lines[i]
                        current_content = current_line.content.strip()
                        
                        if len(cursor_lines) > 0:
                            is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)
                            ## The query of the cursor stands behind its FOR, and it is
                            ## written on the next line as often as not:
                            ##     declare c_top cursor for
                            ##         select ... from #ltv order by ltv desc
                            ## SELECT ends a statement everywhere else, so the declaration
                            ## was cut off in front of its own query - 'c_top cursor for;',
                            ## which PostgreSQL answers with 'missing SQL statement' - and
                            ## the query stayed in the body as a statement of its own.
                            if is_terminator and self.cursor_query_is_incomplete(cursor_lines):
                                is_terminator = None
                            if is_terminator:
                                break
                        cursor_lines.append(current_line.content)
                        i += 1
                        
                    cleaned_lines = [l.strip() for l in cursor_lines]
                    full_cursor = " ".join(cleaned_lines)
                    
                    full_cursor = re.sub(r'^DECLARE\s+', '', full_cursor, flags=re.IGNORECASE)
                    if not full_cursor.strip().endswith(';'):
                        full_cursor = full_cursor.rstrip() + ';'
                        
                    self.cursors.append({
                        "line": start_line,
                        "content": full_cursor
                    })
                    continue
            new_body_lines.append(line)
            i += 1
        self.body_lines = new_body_lines

    def pass_4b_parse_cursor_commands(self):
        self.log("Running Pass 4b: Parse Cursor Commands")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            line_content = line.content.strip()

            if re.match(r'^(OPEN|FETCH|CLOSE|DEALLOCATE)\b', line_content, re.IGNORECASE):
                start_line = line.line_number
                cmd_lines = []
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()
                    if len(cmd_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)
                        if is_terminator:
                            break
                    cmd_lines.append(current_line.content)
                    i += 1
                
                cleaned_lines = [l.strip() for l in cmd_lines]
                full_cmd = " ".join(cleaned_lines)
                
                if re.match(r'^DEALLOCATE\s+CURSOR\b', full_cmd, re.IGNORECASE):
                    full_cmd = f"/* {full_cmd} (Not required in PL/pgSQL) */"
                else:
                    if not full_cmd.strip().endswith(';'):
                        full_cmd += ';'
                        
                self.cursor_commands.append({
                    "line": start_line,
                    "content": full_cmd
                })
            else:
                new_body_lines.append(line)
                i += 1
        self.body_lines = new_body_lines

    def while_condition_is_incomplete(self, condition: str) -> bool:
        """
        Whether the condition of a WHILE collected so far still waits for its continuation on
        the following line.

        Sybase writes the condition of a loop over as many lines as it likes, and the parser
        has to recognize the whole of it: `while (@i < 10` followed by `and @i is not null)`
        is one condition, and taking only its first line left the rest of it standing in the
        body as a statement of its own.
        """
        masked, _ = self.mask_comments_and_literals(condition, False)
        if not masked.strip():
            ## the condition begins on the line after the keyword
            return True
        if masked.count('(') > masked.count(')'):
            return True
        ## a condition which ends on an operator or a boolean connective wants its right side
        return bool(re.search(r'(?i)(\b(AND|OR|NOT|LIKE|IN|BETWEEN|IS)\b|[-+*/%,=<>]|\|\|)\s*$', masked))

    def continues_while_condition(self, line_content: str) -> bool:
        """
        Whether a line continues the condition of the WHILE above it because it begins with a
        boolean connective, an operator or the closing parenthesis of the condition.

        The condition is regularly broken in front of its connective - `while @i < 10` followed
        by `and @j > 0` - where the first line reads as a complete condition on its own.
        """
        if line_content.startswith('--') or line_content.startswith('/*'):
            return False
        return bool(re.match(r'(?i)^(AND|OR)\b|^(\)|\|\||[-+*/%=<>])', line_content))

    def pass_7b_parse_while_loops(self):
        """
        Pass 7b: Parses WHILE loops into `WHILE <condition> LOOP`.

        Sybase writes a loop as `WHILE <condition>` followed by its body, and the condition may
        be attached to the keyword without a space (`while(@i is not null)`) and may run over
        several lines. Only `WHILE <space> <condition on one line>` was recognized, so both of
        these reached the target unconverted - PostgreSQL answered `missing "LOOP" at end of
        SQL expression`, and the `END LOOP;` which Pass 12 adds for the loop had nothing to
        close.
        """
        self.log("Running Pass 7b: Parse WHILE Loops")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            ## the condition may follow the keyword after a space or start right at its '('
            match = re.match(r'^WHILE\b\s*(.*)$', content, re.IGNORECASE)
            if not match:
                new_body_lines.append(line)
                i += 1
                continue

            start_line = line.line_number
            condition_parts = [match.group(1).strip()]
            i += 1

            while i < len(self.body_lines):
                next_content = self.body_lines[i].content.strip()
                if next_content == "":
                    break
                if not (self.while_condition_is_incomplete(" ".join(condition_parts))
                        or self.continues_while_condition(next_content)):
                    break
                condition_parts.append(next_content)
                i += 1

            condition = " ".join(part for part in condition_parts if part).strip()

            ## the BEGIN of the body belongs to the body, not to the condition - it is put back
            ## as a line of its own so that Pass 12 sees the block the loop encloses
            body_begin = None
            if re.search(r'(?<![\w@])BEGIN$', condition, re.IGNORECASE):
                condition = re.sub(r'(?i)(?<![\w@])BEGIN$', '', condition).strip()
                body_begin = type(line)(start_line + 0.1, "BEGIN")

            if not condition:
                ## a WHILE without a condition is not a loop this pass can build - the line is
                ## left in the body, where Pass 10 marks it for the reader
                self.log(f"Pass 7b: WHILE without a condition in line {start_line} - left unconverted")
                new_body_lines.append(line)
                if body_begin is not None:
                    new_body_lines.append(body_begin)
                continue

            self.while_commands.append({
                "line": start_line,
                "content": f"WHILE {condition} LOOP"
            })
            if body_begin is not None:
                new_body_lines.append(body_begin)

        self.body_lines = new_body_lines

    def pass_4_parse_inserts(self):
        """
        Parses INSERT commands.
        Starts with INSERT.
        Ends before next IF, END, UPDATE, RETURN (start of line).
        Rule 64/65: First part ends with ')' or followed by <value>S or SELECT.
        Second part starts with <value>S or SELECT.
        Stores in self.inserts.
        """
        self.log("Running Pass 4: Parse INSERTs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            # Check for INSERT
            # Rule 62: starts with "INSERT"
            if re.match(r'^INSERT\b', content, re.IGNORECASE):
                # Start of INSERT
                start_line = line.line_number
                insert_lines = []

                # Consume lines until terminator found
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    # Check terminator conditions (lines 64)

                    if len(insert_lines) > 0: # Checks for subsequent lines
                        # Check start of line for keywords
                        if re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE):
                             # Terminator found.
                             # But verify continuation first?
                             # Rule: "if line ... ends with ',' or '=' ... next line is part"
                             # Rule: "if next line ... starts with ',' or '=' ... also part"
                             # Rule: "if next line starts with 'FROM' ... also part"

                             # Does continuation override terminator keyword?
                             # Rule 67/68/69 are specific continuation/inclusion rules.
                             # Rule 65 says "ends by empty line or before next IF..."
                             # Usually specific inclusions override general termination.
                             # E.g. if I have `IF` but previous line ended in equals?
                             # Unlikely syntax. But `FROM` is the main one.
                             # If line starts with `FROM`, we include it.
                             pass

                        # Let's invoke termination check, but respect continuation rules
                        # If current_content starts with terminator keyword (IF/END/UPDATE/RETURN)
                        is_terminator = False
                        if current_content == "":
                            next_idx_check = i + 1
                            next_l_check = ""
                            while next_idx_check < len(self.body_lines):
                                next_l_check = self.body_lines[next_idx_check].content.strip()
                                if next_l_check != "":
                                    break
                                next_idx_check += 1
                            if next_l_check:
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                             # Check if we should CONTINUE anyway
                             prev_content = insert_lines[-1].strip()
                             should_continue = False

                             ## An empty line is not what the statement goes on with - the line
                             ## behind it is, and that is the one the rules below ask about. They
                             ## used to ask about the empty line itself, so the SELECT of an
                             ## 'INSERT ... SELECT' written with an empty line in front of it -
                             ## the comments which stood there are taken out by Pass 2 - was not
                             ## recognized as the continuation it is: the INSERT was cut off
                             ## after its list of columns and the SELECT became a statement of
                             ## its own, which the routine then returned as a result set.
                             lookahead = next_l_check if current_content == "" else current_content

                             # Rule 67: prev ends with "," or "="
                             if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                 should_continue = True

                             # Rule 68: next line (current line) starts with "," or "="
                             if lookahead.startswith(",") or lookahead.startswith("="):
                                 should_continue = True

                             # Rule 69: next line (current line) starts with "FROM"
                             if re.match(r'^FROM\b', lookahead, re.IGNORECASE):
                                 should_continue = True

                             # INSERT ... SELECT continuation
                             if re.match(r'^(SELECT|VALUES)\b', lookahead, re.IGNORECASE):
                                 has_values = any(re.search(r'\bVALUES\b', l, re.IGNORECASE) for l in insert_lines)
                                 has_select = any(re.search(r'\bSELECT\b', l, re.IGNORECASE) for l in insert_lines)
                                 if not has_values and not has_select:
                                     should_continue = True

                             ## a CASE expression written over several lines carries its own
                             ## WHEN / ELSE / END, which do not end the statement
                             if self.inside_open_case(insert_lines):
                                 should_continue = True

                             if not should_continue:
                                 break

                    insert_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in insert_lines]
                full_insert = " ".join(cleaned_lines)
                
                # Sybase ASE allows "INSERT table", PostgreSQL requires "INSERT INTO table"
                full_insert = re.sub(r'^INSERT\s+(?!INTO\s+)', 'INSERT INTO ', full_insert, flags=re.IGNORECASE)

                self.inserts.append({
                    "line": start_line,
                    "content": full_insert
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines


    def pass_4c_parse_create_drop_table(self):
        self.log("Running Pass 4c: Parse CREATE and DROP TABLE")
        new_body_lines = []
        i = 0
        import re
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^(CREATE|DROP)\s+TABLE\b', content, re.IGNORECASE):
                cmd_lines = []
                start_line = line.line_number
                
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()
                    if len(cmd_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)
                        if is_terminator:
                            break
                    cmd_lines.append(current_line.content)
                    i += 1
                
                full_cmd = " ".join([l.strip() for l in cmd_lines])
                if not full_cmd.strip().endswith(';'):
                    full_cmd = full_cmd.rstrip() + ';'
                    
                self.exec_commands.append({
                    "line": start_line,
                    "content": full_cmd
                })
                continue
                
            new_body_lines.append(line)
            i += 1
        self.body_lines = new_body_lines

    def pass_5_parse_updates(self):
        """
        Pass 5: Parses UPDATE commands.
        Starts with UPDATE.
        Ends before next IF, ELSE IF, ELSE, END, UPDATE, RETURN.
        """
        self.log("Running Pass 5: Parse UPDATEs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^UPDATE\b', content, re.IGNORECASE):
                start_line = line.line_number
                update_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(update_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)

                        if is_terminator:
                            prev_content = update_lines[-1].strip()
                            should_continue = False

                            # Prev ends with , or =
                            if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                should_continue = True

                            # Curr starts with , or =
                            if current_content.startswith(",") or current_content.startswith("="):
                                should_continue = True

                            # Curr starts with FROM
                            if re.match(r'^FROM\b', current_content, re.IGNORECASE):
                                should_continue = True

                            # UPDATE ... SET continuation
                            if re.match(r'^SET\b', current_content, re.IGNORECASE):
                                has_set = any(re.search(r'\bSET\b', l, re.IGNORECASE) for l in update_lines)
                                if not has_set:
                                    should_continue = True

                            ## a CASE expression written over several lines carries its own
                            ## WHEN / ELSE / END, which do not end the statement
                            if self.inside_open_case(update_lines):
                                should_continue = True

                            if not should_continue:
                                break

                    update_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in update_lines]
                full_update = " ".join(cleaned_lines)

                self.update_commands.append({
                    "line": start_line,
                    "content": full_update
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def pass_5b_parse_deletes(self):
        """
        Pass 5b: Parses DELETE commands.
        Starts with DELETE.
        Ends before next IF, ELSE IF, ELSE, END, UPDATE, INSERT, DELETE, RETURN, SELECT.
        """
        self.log("Running Pass 5b: Parse DELETEs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^DELETE\b', content, re.IGNORECASE):
                start_line = line.line_number
                delete_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(delete_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)

                        if is_terminator:
                            prev_content = delete_lines[-1].strip()
                            should_continue = False

                            # Prev ends with , or =
                            if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                should_continue = True

                            # Curr starts with , or =
                            if current_content.startswith(",") or current_content.startswith("="):
                                should_continue = True

                            # Curr starts with FROM
                            if re.match(r'^FROM\b', current_content, re.IGNORECASE):
                                should_continue = True

                            ## a CASE expression written over several lines carries its own
                            ## WHEN / ELSE / END, which do not end the statement
                            if self.inside_open_case(delete_lines):
                                should_continue = True

                            if not should_continue:
                                break

                    delete_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in delete_lines]
                full_delete = self.normalize_delete_with_second_from(" ".join(cleaned_lines))

                self.delete_commands.append({
                    "line": start_line,
                    "content": full_delete
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def pass_5c_parse_prints(self):
        """
        Pass 5c: Parses PRINT commands.
        Starts with PRINT.
        Ends before next IF, ELSE IF, ELSE, END, UPDATE, INSERT, DELETE, RETURN, SELECT, PRINT.
        Transforms PRINT into RAISE WARNING.
        """
        self.log("Running Pass 5c: Parse PRINTs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^PRINT\b', content, re.IGNORECASE):
                start_line = line.line_number
                print_lines = []
                has_rollback_trigger = False

                # Check if the immediately preceding line was a ROLLBACK
                if len(new_body_lines) > 0:
                    prev_line_content = new_body_lines[-1].content.strip()
                    if re.match(r'^ROLLBACK\s+(TRIGGER|TRANSACTION)\b', prev_line_content, re.IGNORECASE):
                        has_rollback_trigger = True
                        new_body_lines.pop() # Absorb the rollback line

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(print_lines) > 0:
                        # Check if the immediately following line is a ROLLBACK
                        if re.match(r'^ROLLBACK\s+(TRIGGER|TRANSACTION)\b', current_content, re.IGNORECASE):
                            has_rollback_trigger = True
                            i += 1
                            break

                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)

                        if is_terminator:
                            prev_content = print_lines[-1].strip()
                            should_continue = False

                            # Prev ends with , or = or +
                            if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("+") or prev_content.endswith("("):
                                should_continue = True

                            # Curr starts with , or = or +
                            if current_content.startswith(",") or current_content.startswith("=") or current_content.startswith("+"):
                                should_continue = True

                            if not should_continue:
                                break

                    print_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in print_lines]
                full_print = " ".join(cleaned_lines)
                
                # Transform PRINT into RAISE NOTICE or EXCEPTION
                print_args = re.sub(r'^PRINT\b', '', full_print, flags=re.IGNORECASE).strip()
                
                if print_args.startswith("'") or print_args.startswith('"'):
                    match = re.match(r'^((?:\'(?:[^\']|\'\')*\')|(?:"(?:[^"]|"")*"))(.*)$', print_args, re.IGNORECASE)
                    if match:
                        format_str_raw = match.group(1)
                        args = match.group(2).strip()
                        
                        if format_str_raw.startswith('"') and format_str_raw.endswith('"'):
                            # User requested to remove single quotes completely before replacing main double quotes
                            format_str = format_str_raw[1:-1].replace("'", "")
                        elif format_str_raw.startswith("'") and format_str_raw.endswith("'"):
                            # If it was converted by an earlier pass, remove the escaped single quotes
                            format_str = format_str_raw[1:-1].replace("''", "")
                        else:
                            format_str = format_str_raw[1:-1]
                            
                        format_str = re.sub(r'%\d+!', '%', format_str)
                        
                        if args:
                            args = args.lstrip(',').strip()
                            new_print_args = f"'{format_str}', {args}"
                        else:
                            new_print_args = f"'{format_str}'"
                    else:
                        new_print_args = print_args
                else:
                    new_print_args = f"'%', {print_args}"
                
                if has_rollback_trigger:
                    full_print = f"RAISE EXCEPTION {new_print_args}"
                else:
                    full_print = f"RAISE NOTICE {new_print_args}"

                self.print_commands.append({
                    "line": start_line,
                    "content": full_print
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def convert_variable_assignment(self, assignment: str) -> str:
        """
        One `@variable = value` pair of a SET as the assignment `variable := value` of PL/pgSQL.

        T-SQL also writes the assignment together with an operation on the variable
        (`SET @count += 1`), which PL/pgSQL spells out: `count := count + (1)`. The value is
        parenthesized so that an operation of its own keeps its precedence.
        """
        compound = re.match(r'^(@[\w@]+)\s*([-+*/%&|^])=(?!=)\s*(.*)$', assignment, re.DOTALL)
        if compound:
            variable, operator, value = compound.groups()
            return f"{variable} := {variable} {operator} ({value.strip()})"
        return re.sub(r'^(@[\w@]+)\s*=(?!=)', r'\1 :=', assignment, count=1)

    def pass_5d_parse_sets(self):
        """
        Pass 5d: Parses SET commands.

        `SET @var = value` is the assignment of Sybase and becomes `var := value;`. It was left
        in the body untouched, and PostgreSQL read the SET as the one it knows - the one for a
        configuration parameter - and answered at run time with 'unrecognized configuration
        parameter'. Every other SET (SET NOCOUNT ON and its like) changes a behaviour of the
        session which has no counterpart here and is kept as a comment; SET ROWCOUNT is left for
        Pass 11, which turns it into the LIMIT of the statements that follow it.
        """
        self.log("Running Pass 5d: Parse SETs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            is_set = re.match(r'^SET\b', content, re.IGNORECASE)
            is_var = re.match(r'^SET\s+@', content, re.IGNORECASE)
            is_rowcount = re.match(r'^SET\s+ROWCOUNT\b', content, re.IGNORECASE)

            if is_set and is_var:
                start_line = line.line_number
                assignment_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(assignment_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)

                        if is_terminator or current_content == "":
                            prev_content = assignment_lines[-1].content.strip()
                            should_continue = (prev_content.endswith(",") or prev_content.endswith("=")
                                               or prev_content.endswith("(")
                                               or current_content.startswith(",") or current_content.startswith("="))
                            if not should_continue:
                                break

                    assignment_lines.append(current_line)
                    i += 1

                full_set = re.sub(r'\s+', ' ', " ".join(l.content.strip() for l in assignment_lines)).strip()
                full_set = re.sub(r'^SET\s+', '', full_set, count=1, flags=re.IGNORECASE)

                ## `SET @a = 1, @b = 2` are as many assignments as there are pairs, and only the
                ## '=' which follows the variable of a pair is the one of the assignment - a '='
                ## inside the value (a CASE, a comparison) has to stay as it is
                assignments = [self.convert_variable_assignment(part.strip())
                               for part in self.split_outside_parens(full_set)]

                if any(':=' in part for part in assignments):
                    self.set_commands.append({
                        "line": start_line,
                        "content": " ".join(part.rstrip(';') + ';' for part in assignments if part)
                    })
                else:
                    ## a SET on a variable which carries no assignment is none this pass can
                    ## read - the lines stay in the body, where Pass 10 marks them for the reader
                    self.log(f"Pass 5d: SET on a variable without an assignment in line {start_line}")
                    new_body_lines.extend(assignment_lines)
            elif is_set and not is_rowcount:
                start_line = line.line_number
                set_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(set_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE)

                        if is_terminator:
                            prev_content = set_lines[-1].strip()
                            should_continue = False

                            # Prev ends with , or =
                            if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                should_continue = True

                            # Curr starts with , or =
                            if current_content.startswith(",") or current_content.startswith("="):
                                should_continue = True

                            if not should_continue:
                                break

                    set_lines.append(current_line.content)
                    i += 1

                cleaned_lines = [l.strip() for l in set_lines]
                full_set = " ".join(cleaned_lines)
                
                # Transform into comment
                full_set = f"/* {full_set} - Sybase Syntax */"

                self.set_commands.append({
                    "line": start_line,
                    "content": full_set
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def parse_raiserror_statement(self, statement):
        """
        Take the RAISERROR statement apart. Both dialects are written differently:

          Sybase ASE:  RAISERROR <number> [<'format string'> | @variable] [, <argument>, ...]
          MS SQL:      RAISERROR ( <'format string'> | <number>, <severity>, <state> [, <argument>, ...] )

        and the number of Sybase names a message of sysusermessages, which is why a
        RAISERROR very often carries no text at all - 'raiserror 20002, @sku_txt'.

        Returns (error number, message, message is a literal, arguments).
        """
        text = re.sub(r'(?is)^RAISERROR\s*', '', statement.strip().rstrip(';').strip(), count=1)
        ## the options of MS SQL say how the error is reported, not what it says
        text = re.sub(r'(?is)\s+WITH\s+(?:LOG|NOWAIT|SETERROR)(?:\s*,\s*(?:LOG|NOWAIT|SETERROR))*$', '', text).strip()

        parenthesized = False
        if text.startswith('('):
            masked, _ = self.mask_comments_and_literals(text, False)
            depth = 0
            closing = None
            for position, char in enumerate(masked):
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth == 0:
                        closing = position
                        break
            if closing is not None and not text[closing + 1:].strip():
                text = text[1:closing]
                parenthesized = True

        parts = [part.strip() for part in self.split_outside_parens(text, ',')]
        parts = [part for part in parts if part]
        if not parts:
            return None, None, False, []

        error_number = None
        head = parts[0]
        if parenthesized:
            ## message, severity, state, arguments - severity and state are not migrated,
            ## PostgreSQL has no counterpart for them
            message_part = head
            arguments = parts[3:]
        else:
            number_match = re.match(r'(?s)^(\d+)\s*(.*)$', head)
            if number_match:
                error_number = number_match.group(1)
                message_part = number_match.group(2).strip()
            else:
                message_part = head
            arguments = parts[1:]

        message = None
        message_is_literal = False
        if message_part:
            literal = (re.fullmatch(r"(?s)'((?:[^']|'')*)'", message_part)
                       or re.fullmatch(r'(?s)"((?:[^"]|"")*)"', message_part))
            if literal:
                message = literal.group(1)
                message_is_literal = True
            elif parenthesized and re.fullmatch(r'\d+', message_part):
                error_number = message_part
            else:
                ## a variable or an expression holding the text
                message = message_part

        return error_number, message, message_is_literal, arguments

    def convert_message_placeholders(self, message, arguments):
        """
        The placeholders of the message as the '%' of RAISE: the numbered ones of Sybase
        ('%1!', '%2!'), which also say in which order the arguments belong - '%2! %1!'
        really does swap them - and the printf ones of MS SQL ('%s', '%d', '%10.2f').
        A doubled '%%' stands for a percent sign and stays doubled, which is how RAISE
        writes one as well; a single '%' which is none of these is data and has to be
        doubled, otherwise RAISE reads it as a placeholder of its own and refuses the
        statement for the missing value.
        """
        positions = []
        placeholders = [0]

        def replace(match):
            if match.group(0) == '%%':
                return '%%'
            if match.group(1) is not None:
                positions.append(int(match.group(1)))
                placeholders[0] += 1
                return '%'
            if match.group(0) != '%':
                placeholders[0] += 1
                return '%'
            return '%%'

        text = re.sub(r'%%|%(\d+)!|%[-+ #0]*\d*(?:\.\d+)?(?:l|h|I64)?[sdiouxXfeEgGc]|%', replace, message)
        if (positions and len(positions) == len(arguments)
                and sorted(positions) == list(range(1, len(arguments) + 1))):
            arguments = [arguments[position - 1] for position in positions]
        return text, arguments, placeholders[0]

    def build_raise_exception(self, error_number, message, message_is_literal, arguments, statement):
        """
        The RAISE EXCEPTION of the RAISERROR. The message of a number without a text is
        looked up in the messages of the source (sysusermessages, passed in the settings);
        without it the number and the arguments are reported, which is everything the
        routine itself says.
        """
        arguments = [argument for argument in arguments if argument]

        if message is None and error_number is not None:
            user_messages = (self.settings or {}).get('user_messages') or {}
            looked_up = user_messages.get(str(error_number))
            if looked_up:
                message = looked_up
                message_is_literal = True
            else:
                self.log(f"RAISERROR {error_number} carries no message text and the message is not in sysusermessages")
                if self.config_parser:
                    self.config_parser.print_log_message('WARNING',
                        f"tsql_parser: build_raise_exception: '{statement.strip()}' names the message {error_number} of the message catalog of the source, "
                        f"whose text is not available - the exception reports the number and the arguments. "
                        f"Add the text of the message by hand if the application reads it.")

        placeholders = 0
        if message is None:
            text = f"Error {error_number} of the source" if error_number else "Error of the source"
        elif message_is_literal:
            text, arguments, placeholders = self.convert_message_placeholders(message, arguments)
        else:
            ## the message is a variable or an expression - it is printed as the first value
            arguments = [message] + arguments
            text = '%'
            placeholders = 1

        ## the number of values has to match the number of placeholders, RAISE refuses both
        ## 'too few' and 'too many parameters specified for RAISE'
        if len(arguments) > placeholders:
            text += ':' if (placeholders == 0 and arguments) else ''
            text += ' %' * (len(arguments) - placeholders)
        elif placeholders > len(arguments):
            arguments = arguments + ['NULL'] * (placeholders - len(arguments))

        if error_number and message is not None:
            text += f" (error {error_number} of the source)"

        text = text.replace("'", "''")
        if arguments:
            return f"RAISE EXCEPTION '{text}', {', '.join(arguments)}"
        return f"RAISE EXCEPTION '{text}'"

    def pass_5e_parse_raiserror(self):
        """
        Pass 5e: Parses RAISERROR commands, handling preceding ROLLBACK.
        Translates into RAISE EXCEPTION.
        """
        self.log("Running Pass 5e: Parse RAISERRORs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            ## 'rollback trigger with raiserror <number> "<message>"' of Sybase undoes the
            ## work of the trigger and reports the error, and the message is written on the
            ## next line as often as not. It is what RAISE EXCEPTION does in PostgreSQL: the
            ## exception of a trigger function undoes the statement which fired it. Without
            ## this the line was read as a plain ROLLBACK - a statement PL/pgSQL refuses
            ## inside a function - and the message stayed behind as a line of its own.
            if re.match(r'(?i)^ROLLBACK\s+TRIGGER\b', content):
                start_line = line.line_number
                rollback_lines = []
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()
                    if len(rollback_lines) > 0:
                        if current_content == "" or re.match(
                                r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b',
                                current_content, re.IGNORECASE):
                            break
                    rollback_lines.append(current_line.content)
                    i += 1

                statement = " ".join(part.strip() for part in rollback_lines).strip()
                error_part = re.match(r'(?is)^ROLLBACK\s+TRIGGER\s*(?:WITH\s+(RAISERROR\b.*))?$', statement)
                if error_part and error_part.group(1):
                    error_number, message, message_is_literal, arguments = self.parse_raiserror_statement(error_part.group(1))
                    converted = self.build_raise_exception(error_number, message, message_is_literal, arguments, statement)
                else:
                    ## without a message there is nothing to report but the fact itself
                    converted = "RAISE EXCEPTION 'The statement was rolled back by the trigger of the source (rollback trigger)'"
                    if self.config_parser:
                        self.config_parser.print_log_message('WARNING',
                            f"tsql_parser: pass_5e_parse_raiserror: '{statement}' carries no message - it becomes a RAISE EXCEPTION, "
                            f"which undoes the statement the trigger was fired by. PostgreSQL has no way to undo only the work of the "
                            f"trigger and let the statement stand.")

                self.raiserror_commands.append({"line": start_line, "content": converted})
                continue

            if re.match(r'^ROLLBACK\b', content, re.IGNORECASE):
                # Check if next statement is RAISERROR
                next_idx = i + 1
                is_followed_by_raiserror = False
                while next_idx < len(self.body_lines):
                    next_content = self.body_lines[next_idx].content.strip()
                    if next_content != "":
                        if re.match(r'^RAISERROR\b', next_content, re.IGNORECASE):
                            is_followed_by_raiserror = True
                        break
                    next_idx += 1
                
                if is_followed_by_raiserror:
                    # Skip the ROLLBACK
                    i += 1
                    continue
                else:
                    new_body_lines.append(line)
                    i += 1
                    continue

            if re.match(r'^RAISERROR\b', content, re.IGNORECASE):
                start_line = line.line_number
                raiserror_lines = []
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(raiserror_lines) > 0:
                        is_terminator = False
                        if current_content == "":
                            next_idx_check = i + 1
                            next_l_check = ""
                            while next_idx_check < len(self.body_lines):
                                next_l_check = self.body_lines[next_idx_check].content.strip()
                                if next_l_check != "":
                                    break
                                next_idx_check += 1
                            if next_l_check:
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                            break

                    raiserror_lines.append(current_line.content)
                    i += 1

                cleaned_lines = [l.strip() for l in raiserror_lines]
                full_raiserror = " ".join(cleaned_lines)

                error_number, message, message_is_literal, arguments = self.parse_raiserror_statement(full_raiserror)
                full_raiserror = self.build_raise_exception(
                    error_number, message, message_is_literal, arguments, full_raiserror)

                self.raiserror_commands.append({
                    "line": start_line,
                    "content": full_raiserror
                })

                # Check if next statement is RETURN and comment it out
                next_idx = i
                while next_idx < len(self.body_lines):
                    next_line_obj = self.body_lines[next_idx]
                    next_content = next_line_obj.content.strip()
                    if next_content == "":
                        next_idx += 1
                        continue
                    if re.match(r'^RETURN\b', next_content, re.IGNORECASE):
                        self.body_lines[next_idx] = type(next_line_obj)(next_line_obj.line_number, f"/* {next_content} - Sybase syntax */")
                    break

            else:
                new_body_lines.append(line)
                i += 1
        
        self.body_lines = new_body_lines

    def pass_6_parse_selects(self):
        """
        Pass 6: Parses SELECT commands.
        Starts with SELECT.
        Terminates on:
        - Empty line
        - Next line starts with IF, ELSE IF, ELSE, END, BEGIN, UPDATE, INSERT, RETURN.
        Continuation conditions:
        - Line contains only "SELECT"
        - Line ends with ","
        """
        self.log("Running Pass 6: Parse SELECTs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^SELECT\b', content, re.IGNORECASE):
                start_line = line.line_number
                select_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(select_lines) > 0:
                        # Termination check
                        is_terminator = False
                        if current_content == "":
                            is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                            # Continuation logic overrides terminator?
                            # Rule: "if line with SELECT command contains only "SELECT" key word, then next line is also part..."
                            # Rule: "if line ... ends with "," ... then next line is also part..."
                            # Rule: "If next line after SELECT command starts with "FROM" key word, then it is also part of the SELECT command"

                            prev_content = select_lines[-1].strip()

                            # Check continuation conditions
                            is_continuation = False

                            # Standard continuations
                            if prev_content.upper() == "SELECT":
                                is_continuation = True
                            elif prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                is_continuation = True
                            elif re.search(r'\bUNION(?:\s+ALL)?$', prev_content, re.IGNORECASE):
                                is_continuation = True

                            # FROM check override
                            # If we decided to terminate (e.g. valid terminator keyword OR empty line),
                            # check if the *current* line (which is the terminator candidate) is actually FROM?
                            # No, FROM is not in the terminator list.
                            # But if "Ends with empty line" -> current_content is "".
                            # Rule: "If next line ... starts with FROM".
                            # If current is empty, check i+1.
                            if not is_continuation and current_content == "":
                                next_idx_check = i + 1
                                next_l_check = ""
                                while next_idx_check < len(self.body_lines):
                                     next_l_check = self.body_lines[next_idx_check].content.strip()
                                     if next_l_check != "":
                                         break
                                     next_idx_check += 1
                                
                                if next_l_check:
                                     terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b'
                                     if not re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                         is_continuation = True

                            ## a CASE expression written over several lines carries its own
                            ## WHEN / ELSE / END, which do not end the statement
                            if current_content != "" and self.inside_open_case(select_lines):
                                is_continuation = True

                            if not is_continuation:
                                break

                    select_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in select_lines]
                full_select = " ".join(cleaned_lines)

                full_select = re.sub(r'\bnoholdlock\b', '', full_select, flags=re.IGNORECASE)

                self.select_commands.append({
                    "line": start_line,
                    "content": full_select
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def pass_6c_parse_execs(self):
        """
        Pass 6c: Parses EXEC / EXECUTE commands.
        Starts with EXEC or EXECUTE.
        """
        self.log("Running Pass 6c: Parse EXECs")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            if re.match(r'^(EXEC|EXECUTE)\b', content, re.IGNORECASE):
                start_line = line.line_number
                exec_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(exec_lines) > 0:
                        is_terminator = False
                        if current_content == "":
                            next_idx_check = i + 1
                            next_l_check = ""
                            while next_idx_check < len(self.body_lines):
                                next_l_check = self.body_lines[next_idx_check].content.strip()
                                if next_l_check != "":
                                    break
                                next_idx_check += 1
                            if next_l_check:
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                            prev_content = exec_lines[-1].strip()
                            is_continuation = False
                            if prev_content.endswith(","):
                                is_continuation = True
                            
                            if not is_continuation:
                                break
                    
                    exec_lines.append(current_line.content)
                    i += 1

                cleaned_lines = [l.strip() for l in exec_lines]
                full_exec = " ".join(cleaned_lines)

                self.exec_commands.append({
                    "line": start_line,
                    "content": full_exec
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

    def pass_3b_split_inline_ifs(self):
        """
        Pass 3b: Splits inline IF, ELSE and WHILE statements.
        Sybase allows `IF condition command`, `ELSE command` and `WHILE condition command`.
        This pass splits such lines into two: `IF condition` and `command`.

        The WHILE was not split, so `while @i < 10 begin` kept the BEGIN of its body inside the
        condition. The BEGIN was then dropped, the loop was closed after the first statement of
        its body, and the END which the source wrote for that BEGIN was left over at the end of
        the routine. The ELSE was not split either: `else begin` was recognized as neither an
        ELSE nor a statement and reached the target as 'else begin;'.
        """
        self.log("Running Pass 3b: Split Inline IFs, ELSEs and WHILEs")
        new_body_lines = []
        keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "PRINT", "EXEC", "EXECUTE", "BEGIN", "RETURN", "SET", "BREAK", "CONTINUE", "COMMIT", "ROLLBACK", "SAVE"]

        for line in self.body_lines:
            content = line.content.strip()
            if re.match(r'^(IF|ELSE\s+IF|ELSE|WHILE)\b', content, re.IGNORECASE):
                in_single_quote = False
                in_double_quote = False
                paren_level = 0
                split_idx = -1
                
                j = 0
                while j < len(content):
                    char = content[j]
                    
                    if char == "'" and not in_double_quote:
                        in_single_quote = not in_single_quote
                    elif char == '"' and not in_single_quote:
                        in_double_quote = not in_double_quote
                    elif char == '(' and not in_single_quote and not in_double_quote:
                        paren_level += 1
                    elif char == ')' and not in_single_quote and not in_double_quote:
                        paren_level -= 1
                        
                    if not in_single_quote and not in_double_quote and paren_level == 0:
                        if j > 0 and content[j-1].isspace():
                            for kw in keywords:
                                if content[j:].upper().startswith(kw):
                                    end_idx = j + len(kw)
                                    if end_idx == len(content) or (not content[end_idx].isalnum() and content[end_idx] != '_'):
                                        # Special check: UPDATE() is a function, not a command.
                                        if kw == "UPDATE" and content[j:].strip()[len(kw):].strip().startswith("("):
                                            continue
                                        split_idx = j
                                        break
                            if split_idx != -1:
                                break
                    j += 1
                    
                if split_idx != -1:
                    part1 = content[:split_idx].strip()
                    part2 = content[split_idx:].strip()
                    self.log(f"Pass 3b: split inline statement into '{part1}' and '{part2}'")
                    new_body_lines.append(type(line)(line.line_number, part1))
                    new_body_lines.append(type(line)(line.line_number + 0.1, part2))
                else:
                    new_body_lines.append(line)
            else:
                new_body_lines.append(line)

        self.body_lines = new_body_lines

    def pass_7_parse_if_commands(self):
        """
        Pass 7: Parses IF / ELSE IF commands.
        Starts with IF or ELSE IF.
        Ends with ')'.
        Current line ends with ')', and NEXT line is Empty OR starts with BEGIN/SELECT/INSERT/UPDATE/RETURN.
        Removes lines from body.
        Stores in self.if_commands (appends THEN).

        KNOWN LIMITATION: the condition of an IF written over several lines and containing a
        query - `if exists (select * <newline> from inserted ...)` - is not assembled correctly.
        Pass 6 takes the lines of that query out of the body before this pass runs, and the line
        based rule below then ends the condition at the first line which begins with one of the
        statement keywords. The result is a routine whose body does not compile, which
        validate_generated_body() of the connector catches so that the object is reported as
        failed instead of being created. Assembling a statement out of its lines before the
        statements are taken apart is the fix, and it is a change to the order of the passes.
        """
        self.log("Running Pass 7: Parse IF Commands")
        new_body_lines = []
        i = 0
        while i < len(self.body_lines):
            line = self.body_lines[i]
            content = line.content.strip()

            # Check for IF or ELSE IF
            # Regex: start with IF word, or ELSE<spaces>IF word
            if re.match(r'^(IF|ELSE\s+IF)\b', content, re.IGNORECASE):
                start_line = line.line_number
                if_lines = []

                # Consume lines
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip() # for inspection, but keep original indentation

                    if_lines.append(current_line.content)

                    # Check termination:
                    # "ends on the line which is followed either by empty line or line starting with command BEGIN, ELSE, IF, SELECT, INSERT, UPDATE, RETURN"

                    next_idx = i + 1
                    is_terminator = False

                    if next_idx >= len(self.body_lines):
                        # End of body -> Consider terminated?
                        is_terminator = True
                    else:
                        next_line_content = self.body_lines[next_idx].content.strip()
                        if next_line_content == "":
                            is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR|BREAK|CONTINUE|OPEN|FETCH|CLOSE|DEALLOCATE|GOTO)\b', next_line_content, re.IGNORECASE):
                            is_terminator = True

                    if is_terminator:
                        i += 1 # Consume this last line
                        break

                    i += 1

                # No flattening instruction for IF -> Join with newline
                # "space and command THEN must be added"
                # Strip spaces from each line though? Rule 7 says body is stripped.
                # Just join logical lines.
                full_if = "\n".join([l.strip() for l in if_lines])
                full_if += " THEN"

                self.if_commands.append({
                    "line": start_line,
                    "content": full_if
                })
            else:
                new_body_lines.append(line)
                i += 1

        self.body_lines = new_body_lines

        # NEW STEP in Pass 7: Extract isolated ELSE lines
        # "find in the remaining lines of body all lines containing just "ELSE"..."
        final_body_lines = []
        for line in self.body_lines:
            content = line.content.strip()
            if content.upper() == "ELSE":
                self.if_commands.append({
                    "line": line.line_number,
                    "content": content
                })
            else:
                final_body_lines.append(line)

        self.body_lines = final_body_lines

    def split_off_from_clause(self, text):
        """
        Split the text at the FROM which belongs to the statement itself - the one outside
        every parenthesis, string literal and comment, so that the FROM of a subquery
        (`@a = (select max(x) from t)`) does not end the assignment list.
        Returns (text in front of FROM, FROM clause), the second one empty when there is none.
        """
        masked, _ = self.mask_comments_and_literals(text, False)
        for match in re.finditer(r'(?i)\bFROM\b', masked):
            prefix = masked[:match.start()]
            if prefix.count('(') == prefix.count(')'):
                return text[:match.start()].strip(), text[match.start():].strip()
        return text.strip(), ''

    ## 'SELECT @variable = ...' - how Transact-SQL assigns to a variable. It is written like a
    ## query and is not one, which is why the statement converter must not be given it.
    SELECT_ASSIGNMENT = re.compile(r'(?i)^\s*SELECT\s+@[\w@]+\s*=(?!=)')

    def is_select_assignment(self, content):
        """Whether a SELECT command assigns to variables rather than answering with rows."""
        return bool(self.SELECT_ASSIGNMENT.match(re.sub(r'\s+', ' ', content or '')))

    ## A local variable of Transact-SQL. '@@name' is a global one and is not matched - the
    ## connectors convert those before the parser runs. The same rule Pass 9 renames by.
    VARIABLE_REFERENCE = re.compile(r'(?<!@)@([a-zA-Z0-9_]+)')

    def variables_as_identifiers(self, text):
        """
        The '@variables' of a statement written as the identifiers they become in the target,
        so that the statement converter reads them as names.

        Every parser of the Transact-SQL family reads '@id' as a PARAMETER of the statement,
        and the generator for PostgreSQL writes a parameter as '$id'. So every statement of a
        routine which named a variable came back from the conversion with '$id' in it -
        'UPDATE ... WHERE "customer_id" = $cid', 'SELECT $OptIn = 1' - which is not PL/pgSQL
        and which Pass 9 could no longer rename, because the '@' it looks for was gone.
        Renaming them to the name they get anyway ('locvar_cid') hands the converter an
        ordinary identifier instead.

        What stands inside a string literal or a comment is text - 'user@host' is data - so
        the rename is applied only where the statement is SQL.

        Returns the text and the map of the identifiers it introduced, for
        variables_back_from_identifiers() below.
        """
        if not text or '@' not in text:
            return text, {}

        masked, _ = self.mask_comments_and_literals(text, False)
        names = {}
        pieces = []
        position = 0
        for match in self.VARIABLE_REFERENCE.finditer(masked):
            identifier = f"locvar_{match.group(1)}"
            names[identifier.lower()] = identifier
            pieces.append(text[position:match.start()])
            pieces.append(identifier)
            position = match.end()
        pieces.append(text[position:])
        return ''.join(pieces), names

    def variables_back_from_identifiers(self, text, names):
        """
        The variables of a converted statement written the way the DECLARE block writes them.

        The conversion quotes the names it does not know, and a quoted name is NOT folded:
        '"locvar_OptOut"' is a different name from the 'locvar_OptOut' of the DECLARE block,
        which PostgreSQL folds to lower case. The quotes are therefore taken off again and the
        spelling of the declaration is restored, so that the routine reads as it was declared.
        """
        if not text or 'locvar_' not in text.lower():
            return text

        def restore(match):
            found = match.group(1) or match.group(2)
            return names.get(found.lower(), found)

        return re.sub(r'"(locvar_[A-Za-z0-9_]+)"|\b(locvar_[A-Za-z0-9_]+)\b', restore, text,
                      flags=re.IGNORECASE)

    def convert_statement_with_variables(self, statement):
        """
        One statement converted with the statement converter, with its variables carried
        through the conversion as identifiers rather than as parameters.
        """
        prepared, names = self.variables_as_identifiers(statement)
        converted = self.apply_identifier_case(self.view_converter({**self.settings, 'view_code': prepared}))
        if not converted or not converted.strip():
            return converted
        return self.variables_back_from_identifiers(converted, names)

    def convert_assignment_query(self, values_text, from_clause, expected_values):
        """
        The value expressions of a SELECT assignment, converted with the statement converter.

        The assignment itself is not a statement the converter can read, but everything the
        values are made of is: the schema of the source in front of a table, the functions of
        the source, the string concatenation written with '+'. They are converted as the
        SELECT they would be without their targets, and handed back as the list of converted
        values and the converted FROM clause.

        Answers None when there is no converter, when the conversion fails, or when the
        converted statement no longer carries one value per target - in which case the caller
        keeps what the source wrote, which is what happened before anything was converted here
        at all.
        """
        if not self.view_converter or not self.settings:
            return None

        statement = f"SELECT {values_text}"
        if from_clause:
            statement = f"{statement} {from_clause}"

        try:
            converted = self.convert_statement_with_variables(statement)
        except Exception as e:
            self.log(f"Failed to convert the values of a SELECT assignment: {e}")
            return None

        if not converted or not converted.strip():
            self.log("Conversion of the values of a SELECT assignment returned nothing - the original is kept")
            return None

        converted = converted.strip().rstrip(';').strip()
        head = re.match(r'(?is)^SELECT\s+(.*)$', converted)
        if not head:
            self.log(f"The converted values of a SELECT assignment are not a SELECT: {converted[:120]}")
            return None

        converted_values, converted_from = self.split_off_from_clause(head.group(1))
        values = [value.strip() for value in self.split_outside_parens(converted_values, ',')]
        if len(values) != expected_values:
            self.log(f"The conversion changed the number of values of a SELECT assignment "
                     f"({expected_values} -> {len(values)}) - the original is kept")
            return None
        if bool(from_clause) != bool(converted_from):
            self.log("The conversion changed the FROM clause of a SELECT assignment - the original is kept")
            return None
        return values, converted_from

    def pass_8_process_select_assignments(self):
        """
        Pass 8: Processes select assignments.
        Translates `SELECT @var = 1` into `@var := 1;`
        If there are multiple like `SELECT @a = 1, @b = 2`, they get split by `;`
        """
        self.log("Running Pass 8: Process SELECT Assignments")

        for cmd_obj in self.select_commands:
            original_content = cmd_obj['content']

            # Check pattern.
            # Loose check: Starts with SELECT.
            # Needs to contain @variable = value.
            # Regex is tricky for "value" which can be anything.
            # But the rule says "check if it fits pattern SELECT @variable_name = value".
            # "there can be multiple pairs ... separated by ','"

            # Let's clean up spaces first to make regex easier?
            # "replace possible multiple spaces between parts of the pattern with single space"
            # Doing this globally might be safe if we assume we aren't inside string literals?
            # The parser flattened lines joining with space.
            # We should probably respect strings... but for this Pattern check?

            # Simplistic check:
            # Does it look like an assignment?
            # SELECT @var = ...
            # Normalize spaces for check
            normalized = re.sub(r'\s+', ' ', original_content)

            ## The same question Pass 8d asks before it decides not to hand the statement to
            ## the statement converter - one rule, so that a statement cannot fall between the
            ## two and be converted by neither.
            pass_check = self.is_select_assignment(original_content)

            if pass_check:
                # Transform
                # 1. Remove SELECT
                # Case insensitive replace of first SELECT
                cleaned = re.sub(r'^SELECT\s+', '', normalized, count=1, flags=re.IGNORECASE)

                ## An assignment which reads from a table is a query, not an assignment of a
                ## value: 'select @price = list_price from products where ...' became
                ## 'locvar_price := list_price FROM products WHERE ...', which PostgreSQL
                ## cannot read at all - the value of an assignment has no FROM clause. Such a
                ## statement is the SELECT ... INTO of PL/pgSQL, which also sets the row count
                ## the code behind it usually asks for.
                assignments_text, from_clause = self.split_off_from_clause(cleaned)

                ## The values are read apart from their targets whether the statement has a
                ## FROM clause or not, because both shapes need the conversion Pass 8d gives
                ## every other statement and neither may be handed to it whole - see
                ## is_select_assignment(). What the values are made of is converted here, in
                ## the one place which knows which part of the statement is a target and which
                ## is a value.
                assignment_pairs = []
                for part in self.split_outside_parens(assignments_text, ','):
                    pair = re.match(r'(?s)^\s*(@[\w@]+|locvar_[\w]+)\s*=(?!=)\s*(\S.*?)\s*$', part)
                    if not pair:
                        assignment_pairs = []
                        break
                    assignment_pairs.append((pair.group(1), pair.group(2)))

                if assignment_pairs:
                    converted = self.convert_assignment_query(
                        ', '.join(value for _, value in assignment_pairs),
                        from_clause,
                        len(assignment_pairs))
                    if converted:
                        converted_values, converted_from = converted
                        assignment_pairs = [(name, converted_values[index])
                                            for index, (name, _) in enumerate(assignment_pairs)]
                        from_clause = converted_from

                ## An assignment which reads from a table is a query, not an assignment of a
                ## value: 'select @price = list_price from products where ...' became
                ## 'locvar_price := list_price FROM products WHERE ...', which PostgreSQL
                ## cannot read at all - the value of an assignment has no FROM clause. Such a
                ## statement is the SELECT ... INTO of PL/pgSQL, which also sets the row count
                ## the code behind it usually asks for.
                if from_clause:
                    if assignment_pairs:
                        targets = ', '.join(name for name, _ in assignment_pairs)
                        values = ', '.join(value for _, value in assignment_pairs)
                        cmd_obj['content'] = f"SELECT {values} INTO {targets} {from_clause};"
                        continue
                    self.log(f"SELECT assignment with a FROM clause which could not be read: {original_content}")

                elif assignment_pairs:
                    ## every pair is an assignment of its own, in the order the source wrote them
                    cmd_obj['content'] = ' '.join(f"{name} := {value};" for name, value in assignment_pairs)
                    continue

                # 2. Replace , with ; outside of parens/quotes, but stop replacing when hitting a FROM clause
                cleaned = self.replace_commas_outside_parens(cleaned, stop_word="from")

                # 3. Replace = with := for the assignment exclusively (Rule 112)
                # Instead of a global .replace(), target only the assignment operator matching the @variable definition!
                # Every pair of `SELECT @a = 1, @b = 2` is an assignment of its own - converting
                # the first one only left `locvar_b = 2` behind, which PostgreSQL reads as a
                # comparison and reports as a statement it cannot recognize
                cleaned = re.sub(r'(^|;\s*)(@[\w@]+)\s*=(?!=)', r'\1\2 :=', cleaned)

                # 4. Add semicolon at end (Rule 110)
                cleaned = cleaned + ";"

                # Update content
                cmd_obj['content'] = cleaned

    def pass_8b_convert_datetime_formats(self):
        """
        Pass 8b: Converts Sybase ASE convert() calls with format styles into PostgreSQL to_char().
        E.g. convert(varchar(28), dest_commit_time, 9) -> to_char(dest_commit_time, 'Mon DD YYYY HH:MI:SS:MSAM')
        """
        self.log("Running Pass 8b: Convert Datetime Formats")

        style_map = {
            '0': 'Mon DD YYYY HH:MIAM',
            '100': 'Mon DD YYYY HH:MIAM',
            '1': 'MM/DD/YY',
            '101': 'MM/DD/YYYY',
            '2': 'YY.MM.DD',
            '102': 'YYYY.MM.DD',
            '3': 'DD/MM/YY',
            '103': 'DD/MM/YYYY',
            '4': 'DD.MM.YY',
            '104': 'DD.MM.YYYY',
            '5': 'DD-MM-YY',
            '105': 'DD-MM-YYYY',
            '6': 'DD Mon YY',
            '106': 'DD Mon YYYY',
            '7': 'Mon DD, YY',
            '107': 'Mon DD, YYYY',
            '8': 'HH24:MI:SS',
            '108': 'HH24:MI:SS',
            '9': 'Mon DD YYYY HH:MI:SS:MSAM',
            '109': 'Mon DD YYYY HH:MI:SS:MSAM',
            '10': 'MM-DD-YY',
            '110': 'MM-DD-YYYY',
            '11': 'YY/MM/DD',
            '111': 'YYYY/MM/DD',
            '12': 'YYMMDD',
            '112': 'YYYYMMDD',
            '13': 'DD Mon YYYY HH24:MI:SS:MS',
            '113': 'DD Mon YYYY HH24:MI:SS:MS',
            '14': 'HH24:MI:SS',
            '114': 'HH24:MI:SS',
            '20': 'YYYY-MM-DD HH24:MI:SS',
            '120': 'YYYY-MM-DD HH24:MI:SS',
            '21': 'YYYY-MM-DD HH24:MI:SS.MS',
            '121': 'YYYY-MM-DD HH24:MI:SS.MS',
            '140': 'YYYY-MM-DD HH24:MI:SS.US'
        }

        def process_converts_in_string(s):
            result = ""
            i = 0
            while i < len(s):
                # Find next 'convert'
                next_convert = s.lower().find('convert', i)
                if next_convert == -1:
                    result += s[i:]
                    break
                
                # Check if it's a word boundary
                if next_convert > 0 and (s[next_convert-1].isalnum() or s[next_convert-1] == '_'):
                    result += s[i:next_convert+7]
                    i = next_convert + 7
                    continue
                    
                # Match to see if it has '('
                match = re.match(r'convert\s*\(', s[next_convert:], re.IGNORECASE)
                if match:
                    result += s[i:next_convert]
                    start_idx = next_convert
                    args_start = next_convert + match.end()
                    paren_level = 1
                    in_single_quote = False
                    in_double_quote = False
                    args = []
                    current_arg = ""
                    
                    j = args_start
                    while j < len(s):
                        char = s[j]
                        if char == "'" and not in_double_quote:
                            in_single_quote = not in_single_quote
                            current_arg += char
                        elif char == '"' and not in_single_quote:
                            in_double_quote = not in_double_quote
                            current_arg += char
                        elif char == '(' and not in_single_quote and not in_double_quote:
                            paren_level += 1
                            current_arg += char
                        elif char == ')' and not in_single_quote and not in_double_quote:
                            paren_level -= 1
                            if paren_level == 0:
                                args.append(current_arg.strip())
                                break
                            current_arg += char
                        elif char == ',' and paren_level == 1 and not in_single_quote and not in_double_quote:
                            args.append(current_arg.strip())
                            current_arg = ""
                        else:
                            current_arg += char
                        j += 1
                    
                    if paren_level == 0:
                        # Successfully parsed a convert(...) block
                        if len(args) == 2:
                            # convert(type, expr)
                            arg_type = args[0]
                            arg_expr = args[1]
                            replacement = f"CAST({arg_expr} AS {arg_type})"
                            result += replacement
                        elif len(args) == 3:
                            # convert(type, expr, style)
                            arg_type = args[0]
                            arg_expr = args[1]
                            arg_style = args[2]
                            
                            is_char = re.match(r'^(var)?char', arg_type, re.IGNORECASE)
                            pg_format = style_map.get(arg_style, None)
                            
                            if is_char and pg_format:
                                replacement = f"to_char({arg_expr}, '{pg_format}')"
                            else:
                                # Fallback if style isn't mapped or not char, just cast
                                replacement = f"CAST({arg_expr} AS {arg_type})"
                            result += replacement
                        else:
                            # Unknown format, keep as is
                            result += s[start_idx:j+1]
                        
                        i = j + 1
                        continue
                
                result += s[i]
                i += 1
                
            return result

        targets = [
            self.variables,
            self.inserts,
            self.update_commands,
            self.delete_commands,
            self.print_commands,
            self.set_commands,
            self.select_commands,
            self.exec_commands,
            self.if_commands,
            self.comments,
            self.raiserror_commands
        ]

        for line_obj in self.header_lines:
            line_obj.content = process_converts_in_string(line_obj.content)

        for line_obj in self.body_lines:
            line_obj.content = process_converts_in_string(line_obj.content)

        for array in targets:
            for item in array:
                item['content'] = process_converts_in_string(item['content'])

    def pass_8c_process_implicit_returns(self):
        """
        Pass 8c: Prepend RETURN QUERY to non-assignment SELECT statements if implicit_return is True.
        """
        if not getattr(self, 'implicit_return', False):
            return

        self.log("Running Pass 8c: Process Implicit Returns")

        for cmd_obj in self.select_commands:
            original_content = cmd_obj['content']
            normalized = re.sub(r'\s+', ' ', original_content)

            # Do not prepend RETURN QUERY if it's an assignment (e.g. SELECT @var = ...)
            # We assume Pass 8 already handled pure assignments, but we still check.
            is_assignment = bool(re.match(r'^SELECT\s+(@[\w@]+|locvar_[\w]+)\s*(:=|=)', normalized, re.IGNORECASE))
            
            # Do not prepend RETURN QUERY if it has an INTO clause
            has_into = bool(re.search(r'\bINTO\b', normalized, re.IGNORECASE))

            if not is_assignment and not has_into:
                # Prepend RETURN QUERY
                # Use a regex to replace the first 'SELECT' (case-insensitive) with 'RETURN QUERY SELECT'
                new_content = re.sub(r'^(SELECT\s+)', r'RETURN QUERY \1', original_content, count=1, flags=re.IGNORECASE)
                
                # Rule 110: Add semicolon at end if it doesn't already have one
                if not new_content.strip().endswith(';'):
                    new_content += ';'

                cmd_obj['content'] = new_content


    def pass_7c_parse_break_continue(self):
        self.log("Running Pass 7c: Parse BREAK/CONTINUE")
        new_body_lines = []
        for line in self.body_lines:
            line_content = line.content.strip()
            if re.match(r'^BREAK\b', line_content, re.IGNORECASE):
                self.exec_commands.append({
                    "line": line.line_number,
                    "content": "EXIT;"
                })
            elif re.match(r'^CONTINUE\b', line_content, re.IGNORECASE):
                self.exec_commands.append({
                    "line": line.line_number,
                    "content": "CONTINUE;"
                })
            else:
                new_body_lines.append(line)
        self.body_lines = new_body_lines


    def pass_7d_parse_goto_and_labels(self):
        self.log("Running Pass 7d: Parse GOTO and Labels")
        new_body_lines = []
        for line in self.body_lines:
            content = line.content.strip()
            
            m_label = re.match(r'^([a-zA-Z0-9_]+)\s*:$', content)
            if m_label:
                label_name = m_label.group(1)
                self.exec_commands.append({
                    "line": line.line_number,
                    "content": f"/* TODO: LABEL {label_name} */"
                })
                continue
                
            m_goto = re.match(r'^GOTO\s+([a-zA-Z0-9_]+)', content, re.IGNORECASE)
            if m_goto:
                label_name = m_goto.group(1)
                self.exec_commands.append({
                    "line": line.line_number,
                    "content": f"/* TODO: GOTO {label_name} - unsupported in PL/pgSQL */"
                })
                continue
                
            new_body_lines.append(line)
        self.body_lines = new_body_lines

    def apply_identifier_case(self, converted_statement):
        """
        The names of a statement of a routine, spelled the way names_case_handling spelled the
        objects they name.

        This is the same repair the views were given, in the place where the statements inside
        a routine are converted. Without it a trigger of a Sybase ASE or MS SQL Server
        migration held `INSERT INTO "AuditLog" ("OrderId") SELECT NEW."OrderId"` while the
        table is `auditlog` - valid PL/pgSQL which fails the moment the trigger fires, which
        is worse than failing when it is created.

        The records a trigger is given - NEW and OLD - are variables of PL/pgSQL and are always
        folded to lower case, so they are never renamed; the *field* of such a record is the
        column of the table and follows the case handling like any other column.

        A statement which cannot be read is answered exactly as it came in: a name changed by
        a search and replace inside a text nobody could parse is not a conversion.
        """
        if not converted_statement or not converted_statement.strip() or self.config_parser is None:
            return converted_statement
        converted, ok = identifier_case.convert_identifiers(
            converted_statement,
            self.config_parser.convert_names_case,
            self.config_parser.get_source_db_type(),
            keep=identifier_case.PLPGSQL_RESERVED)
        if not ok:
            self.log(f"the case of the names could not be applied to a statement of the routine - "
                     f"it could not be read as PostgreSQL: {converted_statement[:120]}")
            return converted_statement
        return converted

    def pass_8d_convert_selects(self):
        """
        Pass 8d: Converts the statements of the routine with the statement converter, when one
        is provided.

        Only the SELECT commands were converted, so everything a conversion does - the string
        concatenation with '+' to '||', the functions of the source, the schema of the source,
        the quoting of the names - was missing in every INSERT, UPDATE and DELETE of a routine:
        `insert ... select ..., getdate(), @a + ':' + b ...` reached the target as it was
        written for the source and failed there. A statement which the converter cannot read
        keeps its original text and is reported.
        """
        if not self.view_converter or not self.settings:
            return

        self.log("Running Pass 8d: Convert statements with the statement converter")

        ## The query of a cursor is a statement of the routine as well - without the
        ## conversion it kept the names, the functions and the temporary tables of the
        ## source ('select ... from #ltv') in the DECLARE section of the target.
        for cursor in self.cursors:
            declaration = re.match(r'(?is)^(.*?\bCURSOR\b\s*(?:\([^)]*\)\s*)?(?:IS|FOR)\s+)(SELECT\b.*?)(;?)\s*$', cursor['content'])
            if not declaration:
                continue
            try:
                converted_query = self.convert_statement_with_variables(declaration.group(2))
                if converted_query and converted_query.strip():
                    cursor['content'] = f"{declaration.group(1)}{converted_query.strip().rstrip(';')};"
            except Exception as e:
                self.log(f"Failed to convert the query of a cursor: {e}")

        for command_kind, commands in (('SELECT', self.select_commands),
                                       ('INSERT', self.inserts),
                                       ('UPDATE', self.update_commands),
                                       ('DELETE', self.delete_commands)):
            for cmd_obj in commands:
                original_content = cmd_obj['content']

                ## 'SELECT @a = 1, @b = 2' is not a query - it is how Transact-SQL assigns to
                ## its variables, and Pass 8 turns it into the assignments of PL/pgSQL. Handing
                ## it to the statement converter reads '@a' as a parameter of the statement and
                ## writes it as the '$a' of PostgreSQL, so the routine held
                ## 'SELECT $OptIn = 1, $OptOut = 2, ...' - which is not a statement of any
                ## dialect - and Pass 8 no longer recognised the assignment it was meant to
                ## convert, because the '@' it looks for was gone. The values of such a
                ## statement still need the conversion, and Pass 8 asks for it itself.
                if command_kind == 'SELECT' and self.is_select_assignment(original_content):
                    continue

                try:
                    converted = self.convert_statement_with_variables(original_content)

                    if not converted or not converted.strip():
                        self.log(f"Conversion of a {command_kind} command returned nothing - the original is kept")
                        continue

                    if original_content.strip().endswith(';') and not converted.strip().endswith(';'):
                        converted += ';'

                    cmd_obj['content'] = converted
                except Exception as e:
                    self.log(f"Failed to convert {command_kind} command: {e}")


    def pass_8e_convert_pseudo_tables(self):
        """
        Pass 8e: Rewrites the statements which read a pseudo table of a trigger, when the source
        database provides a converter for them.

        A trigger of Sybase ASE and of MS SQL reads the rows of the statement which fired it out
        of the tables 'inserted' and 'deleted', which a trigger of PostgreSQL has as the records
        NEW and OLD instead. The rewriting needs one whole statement at a time - the FROM clause
        the pseudo table is listed in, the columns which belong to it and the condition which
        selects its rows all have to be seen together - and that is what the commands of the
        routine are at this point. It runs after Pass 8d so that the converted statements, whose
        columns are quoted, are the ones rewritten.
        """
        if not self.pseudo_table_converter:
            return

        self.log("Running Pass 8e: Convert the pseudo tables of a trigger")

        for command_kind, commands in (('SELECT', self.select_commands),
                                       ('INSERT', self.inserts),
                                       ('UPDATE', self.update_commands),
                                       ('DELETE', self.delete_commands),
                                       ('IF', self.if_commands),
                                       ('WHILE', self.while_commands),
                                       ('SET', self.set_commands)):
            for cmd_obj in commands:
                content = cmd_obj['content']

                ## the THEN of an IF and the LOOP of a WHILE were added by their pass and are
                ## not part of the statement the converter reads
                suffix = ''
                for keyword in (' THEN', ' LOOP'):
                    if content.upper().endswith(keyword):
                        content, suffix = content[:-len(keyword)], content[-len(keyword):]
                        break

                try:
                    converted = self.pseudo_table_converter(content, command_kind)
                except Exception as e:
                    self.log(f"Failed to convert the pseudo tables of a {command_kind} command: {e}")
                    continue

                if converted and converted != content:
                    cmd_obj['content'] = converted + suffix

    def pass_9_rename_variables(self):
        """
        ...
        Also transforms IF (@@sqlstatus!=0) BREAK into EXIT WHEN NOT FOUND;
        """

        """
        Pass 9: Global replacement of local variable notation.
        Iterate over all arrays and replace @var with locvar_var.
        Ignore @@var.
        Case-insensitive check, preserve case of var.
        """
        self.log("Running Pass 9: Rename Variables")

        def apply_rename(content):
            ## The rename is applied where the line is SQL and nowhere else. It used to be a
            ## plain re.sub over the whole line, so an '@' inside a string literal was renamed
            ## as if it were a variable: 'mail to admin@example.com' was written into the
            ## target as 'mail to adminlocvar_example.com'. That is data, and a routine which
            ## rewrites the text it inserts is worse than one which does not compile.
            return self.variables_as_identifiers(content)[0]

        # Arrays to process:
        # header_lines, body_lines (remaining), insert_commands, update_commands, select_commands, if_commands
        # AND variables (declarations) - based on implementation plan analysis.

        # List[SourceLine] arrays
        for line_obj in self.header_lines:
            line_obj.content = apply_rename(line_obj.content)

        for line_obj in self.body_lines:
            line_obj.content = apply_rename(line_obj.content)

        # List[Dict] arrays (key 'content')
        targets = [
            self.variables,
            self.inserts,
            self.update_commands,
            self.delete_commands,
            self.print_commands,
            self.set_commands,
            self.select_commands,
            self.exec_commands,
            self.if_commands,
            self.comments,
            self.cursors,
            self.cursor_commands,
            self.while_commands,
            self.raiserror_commands
        ]

        for array in targets:
            for item in array:
                item['content'] = apply_rename(item['content'])

        ## @@sqlstatus reports the result of the last FETCH, and the loop of a cursor tests it
        ## just as an IF does - `while @@sqlstatus = 0` is the regular way to read a cursor out
        for cmd in self.if_commands + self.while_commands:
            cmd['content'] = re.sub(r'@@sqlstatus\s*!=\s*0', 'NOT FOUND', cmd['content'], flags=re.IGNORECASE)
            cmd['content'] = re.sub(r'@@sqlstatus\s*=\s*0', 'FOUND', cmd['content'], flags=re.IGNORECASE)



    def print_all_arrays(self, final_output: List[str]):
        print(f"--- START CHECKS FOR {self.filepath} ---")

        print(f"--- HEADER ARRAY ({len(self.header_lines)} lines) ---")
        for l in self.header_lines:
            print(f"Line {l.line_number}: {l.content}")

        print(f"--- COMMENTS ARRAY ({len(self.comments)} items) ---")
        for c in self.comments:
            print(f"Line {c['line']}: {c['content']}")

        print(f"--- VARIABLES DECLARATION ARRAY ({len(self.variables)} items) ---")
        for v in self.variables:
            print(f"Line {v['line']}: {v['content']}")

        print(f"--- INSERTS ARRAY ({len(self.inserts)} items) ---")
        for i in self.inserts:
            print(f"Line {i['line']}: {i['content']}")

        print(f"--- UPDATE COMMANDS ARRAY ({len(self.update_commands)} items) ---")
        for u in self.update_commands:
            print(f"Line {u['line']}: {u['content']}")

        print(f"--- DELETE COMMANDS ARRAY ({len(self.delete_commands)} items) ---")
        for d in self.delete_commands:
            print(f"Line {d['line']}: {d['content']}")

        print(f"--- PRINT COMMANDS ARRAY ({len(self.print_commands)} items) ---")
        for p in self.print_commands:
            print(f"Line {p['line']}: {p['content']}")

        print(f"--- SET COMMANDS ARRAY ({len(self.set_commands)} items) ---")
        for st in self.set_commands:
            print(f"Line {st['line']}: {st['content']}")

        print(f"--- SELECT COMMANDS ARRAY ({len(self.select_commands)} items) ---")
        for s in self.select_commands:
            print(f"Line {s['line']}: {s['content']}")

        print(f"--- EXEC COMMANDS ARRAY ({len(self.exec_commands)} items) ---")
        for e in self.exec_commands:
            print(f"Line {e['line']}: {e['content']}")

        print(f"--- IF COMMANDS ARRAY ({len(self.if_commands)} items) ---")
        for f in self.if_commands:
            print(f"Line {f['line']}: {f['content']}")

        print(f"--- REMAINING BODY LINES ARRAY ({len(self.body_lines)} lines) ---")
        for b in self.body_lines:
            print(f"Line {b.line_number}: {b.content}")

        print(f"--- FINAL OUTPUT ARRAY ({len(final_output)} lines) ---")
        for idx, l in enumerate(final_output):
             print(f"{l}")

        print(f"--- END CHECKS FOR {self.filepath} ---")


    def pass_9b_process_rowcount(self):
        """
        Pass 9b: Reads @@rowcount of Sybase out of the row count of the last statement.

        A condition of a WHILE is tested once per turn of the loop, so the row count it reads
        has to be taken again at the end of the body - the reading in front of the loop alone
        describes the first turn only. Pass 12 adds that second reading when it closes the loop.
        """
        self.log("Running Pass 9b: Process @@rowcount")
        used_rowcount = False
        ## Every statement reading @@rowcount needs the row count taken in front of it, not
        ## only a condition: 'select @deleted = @@rowcount' kept the variable of the source
        ## and reached the target as 'locvar_deleted := @@rowcount', which PostgreSQL cannot
        ## read at all.
        reading_commands = (self.if_commands + self.while_commands + self.select_commands
                            + self.set_commands + self.print_commands + self.exec_commands
                            + self.inserts + self.update_commands + self.delete_commands)
        for cmd in list(reading_commands):
            if re.search(r'@@rowcount', cmd['content'], re.IGNORECASE):
                used_rowcount = True
                cmd['content'] = re.sub(r'@@rowcount', 'locvar_rowcount', cmd['content'], flags=re.IGNORECASE)
                self.exec_commands.append({
                    "line": cmd['line'] - 0.1,
                    "content": "GET DIAGNOSTICS locvar_rowcount = ROW_COUNT;"
                })

        for line_obj in self.body_lines:
            if re.search(r'@@rowcount', line_obj.content, re.IGNORECASE):
                used_rowcount = True
                line_obj.content = re.sub(r'@@rowcount', 'locvar_rowcount', line_obj.content, flags=re.IGNORECASE)
                self.exec_commands.append({
                    "line": line_obj.line_number - 0.1,
                    "content": "GET DIAGNOSTICS locvar_rowcount = ROW_COUNT;"
                })

        if used_rowcount:
            # Check if locvar_rowcount is already in variables
            found = False
            for v in self.variables:
                if 'locvar_rowcount' in v['content']:
                    found = True
                    break
            if not found:
                self.variables.append({
                    "line": 0,
                    "content": "locvar_rowcount INTEGER;"
                })


    def pass_10_add_semicolons(self):
        """
        Pass 10 (New): Checks remaining body lines.
        Add ';' to END, RETURN.
        Mark others as TODO.
        """
        self.log("Running Pass 10: Add Semicolons")

        for line in self.body_lines:
            content = line.content.strip()

            # Rule: "if line contains only empty line or only spaces, remove all spaces and keep it as totally empty line"
            if not content:
                line.content = ""
                continue

            # Rule 120: END (without semicolon) -> add semicolon
            if re.match(r'^END\b', content, re.IGNORECASE) and not content.endswith(';'):
                 # Check if it contains ONLY "END" (Rule 120: "if line contains END key word only")
                 # We already stripped spaces.
                 if content.upper() == "END":
                     line.content = content + ";"
                     continue

            # Rule 121: RETURN (only or with value) -> add semicolon
            if re.match(r'^RETURN\b', content, re.IGNORECASE) and not content.endswith(';'):
                upper_c = content.upper()
                case_count = len(re.findall(r'\bCASE\b', upper_c))
                end_count = len(re.findall(r'\bEND\b', upper_c))
                paren_open = content.count('(')
                paren_close = content.count(')')
                if case_count > end_count or paren_open > paren_close:
                    continue
                if re.search(r'\b(WHEN|THEN|ELSE|AND|OR|\+|\-|\*|/)\s*$', content, re.IGNORECASE):
                    continue
                line.content = content + ";"
                continue

            # Rule 122: BEGIN -> keep unchanged
            if re.match(r'^BEGIN\b', content, re.IGNORECASE) and content.upper() == "BEGIN":
                continue

            # Convert Sybase BEGIN TRAN(SACTION) to NULL; comment to prevent breaking IF block symmetry
            if re.match(r'^BEGIN\s+TRAN(SACTION)?\b', content, re.IGNORECASE):
                line.content = f"NULL; /* {content} (Not required in PL/pgSQL) */"
                continue

            # Convert Sybase SAVE TRAN(SACTION) to NULL; comment
            if re.match(r'^SAVE\s+TRAN(SACTION)?\b', content, re.IGNORECASE):
                line.content = f"NULL; /* {content} (Not required in PL/pgSQL) */"
                continue

            # Convert Sybase COMMIT TRAN(SACTION) to simple COMMIT
            if re.match(r'^COMMIT(\s+TRAN(SACTION)?)?\b', content, re.IGNORECASE):
                line.content = "COMMIT;"
                continue

            # Convert Sybase ROLLBACK TRAN(SACTION) to simple ROLLBACK
            if re.match(r'^ROLLBACK(\s+TRAN(SACTION)?)?\b', content, re.IGNORECASE):
                line.content = "ROLLBACK;"
                continue

            # Allow SET ROWCOUNT to pass through cleanly so it can be handled by pass 11
            if re.match(r'^SET\s+ROWCOUNT\b', content, re.IGNORECASE):
                continue

            # Skip lines that are already comments
            if content.startswith('--') or content.startswith('/*'):
                continue

            # Clause continuation lines or lines already ending with ';' should not get a TODO tag or duplicate ';'
            if content.endswith(';') or re.match(r'^(THEN|ELSE|WHEN|END|AND|OR|UPDATE|INSERT|DELETE|MERGE|SELECT)\b', content, re.IGNORECASE):
                if not line.content.rstrip().endswith(';'):
                    line.content = line.content.rstrip() + ";"
                continue

            # Rule 123: Anything else -> add TODO
            # If we are here, it's a TODO line.
            line.content = line.content.rstrip(';') + "; /* TODO: not processed line - check syntax */"

    def pass_11_assemble_output(self, pg_header_str=None) -> List[OutputLine]:
        """
        Pass 11: Combining the result.
        Creates output array of OutputLine objects.
        """
        self.log("Running Pass 11: Assemble Output")

        output_array: List[OutputLine] = []
        current_new_line_num = 1

        # Helper to add line
        def add_line(source_name, original_num, text):
            nonlocal current_new_line_num
            output_array.append(OutputLine(current_new_line_num, source_name, original_num, text))
            current_new_line_num += 1

        # 1. Header lines
        if pg_header_str:
            for line in pg_header_str.split("\\n"):
                add_line("header", 0, line)
        else:
            for line in self.header_lines:
                add_line("header", line.line_number, line.content)

        add_line("separator", 0, "$$")

        # 3. DECLARE
        if self.variables or self.cursors:
            add_line("declare_section", 0, "DECLARE")

            # 4. Variables
            for var in self.variables:
                add_line("variable_declaration", var['line'], var['content'])


            # 4b. Cursors
            for c in self.cursors:
                content = c['content']
                ## the name of a temporary table has no '#' in the target
                content = re.sub(r"(?<!')#([a-zA-Z0-9_]+)\b", r'\1', content)
                ## PostgreSQL has no such clause - its cursors are read only, and an update
                ## through one is written as 'WHERE CURRENT OF' without declaring it here
                content = re.sub(r'(?is)\s+FOR\s+(?:READ\s+ONLY|UPDATE(?:\s+OF\s+[^;]+)?)\s*;?\s*$', ';', content)
                add_line("cursor_declaration", c['line'], content)

        # 5. Body Output Array
        # Collect all parts, sort by original line number, then append.

        # Structure: (line_number, content, source_name)
        body_parts = []

        for l in self.body_lines:
            body_parts.append((l.line_number, l.content, "remaining_body_lines"))


        for cmd in self.cursor_commands:
            body_parts.append((cmd['line'], cmd['content'], "cursor_commands"))

        for w in self.while_commands:
            body_parts.append((w['line'], w['content'], "while_commands"))

        for i in self.inserts:
            content = i['content']
            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((i['line'], content, "insert_commands"))

        for u in self.update_commands:
            content = u['content']
            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((u['line'], content, "update_commands"))

        for d in self.delete_commands:
            content = d['content']
            
            match_from = re.match(r'^DELETE\s+([#a-zA-Z0-9_]+)\s+FROM\s+(.+?)(?=\s+WHERE\b|\s*$)', content, re.IGNORECASE)
            if match_from:
                content = re.sub(r'^DELETE\s+([#a-zA-Z0-9_]+)\s+FROM\s+', r'DELETE FROM \1 USING ', content, count=1, flags=re.IGNORECASE)
            else:
                match_where = re.match(r'^DELETE\s+([#a-zA-Z0-9_]+)\s+(WHERE\b)', content, re.IGNORECASE)
                if match_where:
                    content = re.sub(r'^DELETE\s+([#a-zA-Z0-9_]+)\s+WHERE\b', r'DELETE FROM \1 WHERE', content, count=1, flags=re.IGNORECASE)

            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((d['line'], content, "delete_commands"))

        for p in self.print_commands:
            content = p['content']
            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((p['line'], content, "print_commands"))

        for st in self.set_commands:
            content = st['content']
            # A SET kept as a comment needs no semicolon, an assignment out of SET @var = value does
            if not content.strip().startswith('/*') and not content.strip().endswith(';'):
                content += ';'
            body_parts.append((st['line'], content, "set_commands"))

        for r in self.raiserror_commands:
            content = r['content']
            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((r['line'], content, "raiserror_commands"))

        for s in self.select_commands:
            content = s['content']
            if not content.strip().endswith(';'):
                content += ';'
            body_parts.append((s['line'], content, "select_commands"))

        for e in self.exec_commands:
            content = e['content']
            
            # Convert EXEC procedure_name [args] to PERFORM procedure_name(args)
            exec_match = re.match(r'^(EXECUTE|EXEC)\s+(.+)$', content, re.IGNORECASE)
            if exec_match:
                remainder = exec_match.group(2).strip()
                
                # Exclude dynamic SQL like EXECUTE ('...') or EXECUTE (locvar_...)
                if not remainder.startswith('(') and not remainder.startswith('\''):
                    assign_match = re.match(r'^(@[a-zA-Z0-9_]+|locvar_[a-zA-Z0-9_]+)\s*=\s*(.+)$', remainder)
                    if assign_match:
                        var_name = assign_match.group(1)
                        rest = assign_match.group(2).strip()
                        parts = re.split(r'\s+', rest, maxsplit=1)
                        proc_name = '.'.join([f'"{p}"' for p in parts[0].split('.')])
                        args = parts[1].strip() if len(parts) > 1 else ""
                        content = f"{var_name} := {proc_name}({args})"
                    else:
                        # Split into procedure name and arguments
                        parts = re.split(r'\s+', remainder, maxsplit=1)
                        proc_name = '.'.join([f'"{p}"' for p in parts[0].split('.')])
                        args = parts[1].strip() if len(parts) > 1 else ""
                        content = f"PERFORM {proc_name}({args})"
                else:
                    # Keep as EXECUTE for dynamic SQL
                    content = f"EXECUTE {remainder}"

            if not content.strip().endswith(';') and not content.strip().startswith('/*'):
                content += ';'
            body_parts.append((e['line'], content, "exec_commands"))

        for f in self.if_commands:
            content = f['content']
            # Rule 138: replace "ELSE IF" with "ELSIF"
            content = re.sub(r'ELSE\s+IF', 'ELSIF', content, flags=re.IGNORECASE)
            # Rule 139: "ELSIF(" -> "ELSIF ("
            content = re.sub(r'ELSIF\(', 'ELSIF (', content, flags=re.IGNORECASE)

            body_parts.append((f['line'], content, "if_commands"))

        for c in self.comments:
            body_parts.append((c['line'], c['content'], "comments"))

        # Sort by line number
        body_parts.sort(key=lambda x: x[0])

        # Apply SET ROWCOUNT limit to subsequent SELECT commands
        active_rowcount_limit = None
        new_body_parts = []
        for line_num, content, source_name in body_parts:
            # Detect SET ROWCOUNT N
            ## The number of rows may be held in a variable - 'set rowcount @top_n' - which
            ## is a LIMIT of PostgreSQL just as well. Only the written number was read, so
            ## such a line stayed in the code as it was written for the source.
            m = re.match(r'^\s*SET\s+ROWCOUNT\s+(\d+|locvar_[a-zA-Z0-9_]+|@[a-zA-Z0-9_]+)', content, re.IGNORECASE)
            if m:
                limit_val = m.group(1)
                active_rowcount_limit = None if limit_val == '0' else limit_val
                content = re.sub(r'(?i)^\s*SET\s+ROWCOUNT\s+(?:\d+|locvar_[a-zA-Z0-9_]+|@[a-zA-Z0-9_]+)',
                                 f'/* SET ROWCOUNT {limit_val} converted to LIMIT */', content)

            # Apply LIMIT to SELECT commands
            elif source_name == "select_commands" and active_rowcount_limit:
                if not re.search(r'\bLIMIT\s+\d+', content, re.IGNORECASE):
                    if content.strip().endswith(';'):
                        content = content.strip()[:-1] + f" LIMIT {active_rowcount_limit};"
                    else:
                        content = content.strip() + f" LIMIT {active_rowcount_limit}"

            # Strip # prefix from temporary table identifiers (avoids string literals starting with ')
            content = re.sub(r"(?<!')#([a-zA-Z0-9_]+)\b", r"\1", content)

            new_body_parts.append((line_num, content, source_name))

        body_parts = new_body_parts

        # Inject BEGIN and END if they are missing from the Sybase source
        ## A comment is not a statement of the routine - a body which begins or ends with one
        ## still begins with its BEGIN and ends with its END. Counting the comment as the
        ## first or the last line added a second END behind it, which PostgreSQL answers with
        ## 'syntax error at end of input'.
        def is_comment_only(text):
            stripped = text.strip()
            return stripped.startswith('--') or (stripped.startswith('/*') and stripped.endswith('*/'))

        injected_begin = False
        if body_parts:
            first_content = next((x[1].strip().upper() for x in body_parts
                                  if x[1].strip() and not is_comment_only(x[1])), "")
            if first_content != "BEGIN":
                add_line("injected_begin", 0, "BEGIN")
                injected_begin = True

        # Append to output_array
        for item in body_parts:
            add_line(item[2], item[0], item[1])

        if body_parts:
            last_content = next((x[1].strip().upper() for x in reversed(body_parts)
                                 if x[1].strip() and not is_comment_only(x[1])), "")
            if injected_begin or last_content not in ("END", "END;"):
                add_line("injected_end", 0, "END;")

        # 6. Final Separator
        add_line("separator", 0, "$$ LANGUAGE plpgsql;")

        return output_array

    def pass_12_add_if_levels(self, output_array: List[OutputLine]):
        """
        Pass 12: Adding levels of IF commands and END IF; commands.
        """
        self.log("Running Pass 12: Add IF Levels and END IF; commands")
        new_array = []
        if_stack = []
        
        for i, line in enumerate(output_array):
            content = line.content.strip()
            
            # Skip empty lines, top level separators, and comments
            if content == "" or content == "$$" or content == "$$ LANGUAGE plpgsql;" or content.startswith("/*"):
                new_array.append(line)
                continue
                
            # If we are expecting a target for the top of the stack...
            if if_stack and if_stack[-1]["state"] == "EXPECT_TARGET":
                if re.match(r'^BEGIN\b', content, re.IGNORECASE):
                    if_stack[-1]["state"] = "INSIDE_BEGIN"
                else:
                    # It's a single statement! It completes the target immediately!
                    if_stack[-1]["state"] = "TARGET_COMPLETED"
                    
            # Check what the current line is
            if re.match(r'^IF\b', content, re.IGNORECASE):
                # New IF statement starts
                if_stack.append({"type": "IF", "state": "EXPECT_TARGET"})
                new_array.append(line)
                

            elif re.match(r'^WHILE\b', content, re.IGNORECASE):
                if_stack.append({"type": "WHILE", "state": "EXPECT_TARGET", "condition": content})
                new_array.append(line)
            elif re.match(r'^(ELSIF|ELSE\s+IF|ELSE)\b', content, re.IGNORECASE):
                # Extends the current IF statement
                if if_stack:
                    if_stack[-1]["state"] = "EXPECT_TARGET"
                new_array.append(line)
                
            elif re.match(r'^END;', content, re.IGNORECASE):
                new_array.append(line)
                # This closes a BEGIN block.
                # Does it complete a target for the current IF?
                if if_stack and if_stack[-1]["state"] == "INSIDE_BEGIN":
                    if_stack[-1]["state"] = "TARGET_COMPLETED"
                    
            else:
                # Normal statement. We already handled EXPECT_TARGET above.
                new_array.append(line)

            # Check if we should close any completed IF statements.
            # Look ahead to the next non-empty line
            next_content = ""
            for j in range(i + 1, len(output_array)):
                nxt = output_array[j].content.strip()
                if nxt != "" and not nxt.startswith("/*"):
                    next_content = nxt
                    break
            
            while if_stack and if_stack[-1]["state"] == "TARGET_COMPLETED":
                if re.match(r'^(ELSIF|ELSE\s+IF|ELSE)\b', next_content, re.IGNORECASE):
                    # The IF is extended by an ELSE/ELSIF block, do not close it yet
                    break
                else:
                    # Close the IF!
                    popped_item = if_stack.pop()
                    orig_line = getattr(line, 'original_line_number', getattr(line, 'line_number_approx', 0))
                    if popped_item["type"] == "WHILE":
                        ## a loop which turns on the row count reads it again for the next turn
                        if 'locvar_rowcount' in popped_item.get("condition", ""):
                            new_array.append(type(line)(0, 'injected_rowcount_refresh', orig_line,
                                                        "GET DIAGNOSTICS locvar_rowcount = ROW_COUNT;"))
                        new_line = type(line)(0, 'injected_end_loop', orig_line, "END LOOP;")
                    else:
                        new_line = type(line)(0, 'injected_end_if', orig_line, "END IF;")
                    new_array.append(new_line)

        # Update the output array
        output_array.clear()
        output_array.extend(new_array)

    def print_with_indentation(self, output_file: str, final_lines: List[OutputLine]):
        """
        Prints output array to file with indentation rules.
        """
        with open(output_file, 'w') as f:
            indent_level = 0

            in_body = False
            first_begin_found = False

            def get_indent(level):
                return "    " * max(0, level)

            for line_obj in final_lines:
                stripped = line_obj.content.strip()
                current_indent = indent_level

                # Rule 162: "$$" or "DECLARE" -> level 0
                if stripped.upper() == "DECLARE":
                    indent_level = 0
                    in_body = True
                    f.write(get_indent(0) + line_obj.content + "\n")
                    # Rule 163: between DECLARE and first body line -> level 1
                    indent_level = 1
                    continue

                if stripped == "$$":
                    indent_level = 0
                    f.write(get_indent(0) + line_obj.content + "\n")
                    in_body = True
                    continue

                if stripped.upper() == "$$ LANGUAGE PLPGSQL;":
                    indent_level = 0
                    f.write(get_indent(0) + line_obj.content + "\n")
                    continue

                # Header lines (before body)
                if not in_body:
                    current_indent = 0
                    f.write(get_indent(0) + line_obj.content + "\n")
                    continue

                # Rule 164: First BEGIN -> level 0
                if re.match(r'^BEGIN\b', stripped, re.IGNORECASE):
                    if not first_begin_found:
                         first_begin_found = True
                         current_indent = 0
                         # Rule 165: next line increases by 1
                         indent_level = 1
                    else:
                         # Subsequent BEGIN
                         current_indent = indent_level
                         indent_level += 1

                # Rule 167: END; -> decreases level
                elif re.match(r'^END;', stripped, re.IGNORECASE):
                     indent_level -= 1
                     current_indent = indent_level
                     if indent_level < 0:
                         # Rule 168/158 compliance
                         indent_level = 0
                         current_indent = 0

                else:
                    # Rule 166: IF/ELSE... no change
                    pass

                # Write
                f.write(get_indent(current_indent) + line_obj.content + "\n")



    def run(self, pg_header_str=None):
        self.log(f"Processing parsing passes...")
        self.read_code()
        self.parse_header_and_body_boundary()

        # Pass 1
        self.pass_1_split_inline_comments()

        # Pass 0b
        self.pass_0b_map_tempdb()

        # Pass 1c
        self.pass_1c_split_inline_goto()

        # Pass 2
        self.pass_2_extract_comments()

        # Pass 3
        self.pass_3_parse_variables()

        # Pass 3c
        self.pass_3c_parse_cursors()

        # Pass 3b
        self.pass_3b_split_inline_ifs()

        # Pass 3d
        self.pass_7d_parse_goto_and_labels()

        # Pass 4
        self.pass_4_parse_inserts()

        # Pass 4b
        self.pass_4b_parse_cursor_commands()

        # Pass 4c
        self.pass_4c_parse_create_drop_table()

        # Pass 5
        self.pass_5_parse_updates()

        # Pass 5b
        self.pass_5b_parse_deletes()

        # Pass 5c
        self.pass_5c_parse_prints()

        # Pass 5d
        self.pass_5d_parse_sets()

        # Pass 5e
        self.pass_5e_parse_raiserror()

        # Pass 6
        self.pass_6_parse_selects()

        # Pass 6c
        self.pass_6c_parse_execs()

        # Pass 7
        self.pass_7_parse_if_commands()

        # Pass 7b
        self.pass_7b_parse_while_loops()

        # Pass 7c
        self.pass_7c_parse_break_continue()


        # Pass 8d
        self.pass_8d_convert_selects()

        # Pass 8e
        self.pass_8e_convert_pseudo_tables()

        # Pass 9b
        self.pass_9b_process_rowcount()

        # Pass 8
        self.pass_8_process_select_assignments()

        # Pass 8b
        self.pass_8b_convert_datetime_formats()

        # Pass 8c
        self.pass_8c_process_implicit_returns()

        # Pass 9
        self.pass_9_rename_variables()

        # Pass 10
        self.pass_10_add_semicolons()

        # Pass 11
        final_output = self.pass_11_assemble_output(pg_header_str)

        # Pass 12
        self.pass_12_add_if_levels(final_output)

        # Apply global SQL functions mapping (e.g., datepart, getdate) across all output lines
        if self.functions_mapping_converter:
            ## The type of the target database, not the way it is connected to -
            ## get_connectivity() answers 'native' / 'jdbc' / 'odbc' as a string, and asking a
            ## string for a key ended the whole conversion of the routine with
            ## "'str' object has no attribute 'get'".
            target_db_type = 'postgresql'
            if self.config_parser:
                target_db_type = self.config_parser.get_target_db_type() or 'postgresql'
            for line in final_output:
                line.content = self.functions_mapping_converter(line.content, {'target_db_type': target_db_type})

        return final_output