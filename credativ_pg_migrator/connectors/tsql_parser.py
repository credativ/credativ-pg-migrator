import re
import os
import glob
from typing import List, Dict, Any

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
    def __init__(self, code_str: str, config_parser=None, implicit_return=False, view_converter=None, settings=None, functions_mapping_converter=None):
        self.code_str = code_str
        self.config_parser = config_parser
        self.implicit_return = implicit_return
        self.view_converter = view_converter
        self.settings = settings
        self.functions_mapping_converter = functions_mapping_converter
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
        self.pass_2_extract_comments()
        self.pass_3_parse_variables()
        self.pass_3b_split_inline_ifs()
        self.pass_4_parse_inserts()
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

    def parse_header_and_body_boundary(self):
        """
        Identify where the header ends and the body begins.
        Header ends at 'AS'. Body starts after 'AS'.
        Body ends at 'END'.
        """
        # Parsing of header
        # Header starts with "CREATE PROCEDURE" or "CREATE FUNCTION" or "CREATE TRIGGER" ... ends with "AS" key word

        as_index = -1
        end_index = -1

        # Determine body start (after 'AS')
        for i, line in enumerate(self.raw_lines):
            # Check for isolated AS
            match = re.search(r'\bAS\b', line.content, re.IGNORECASE)
            if match:
                as_index = i
                # Check if there's anything after 'AS'
                after_as = line.content[match.end():]
                if after_as.strip():
                    # Split the line into two SourceLines
                    header_part = line.content[:match.end()]
                    body_part = after_as

                    # Update current line to be just the header part
                    self.raw_lines[i].content = header_part

                    # Insert the rest as the next line (preserve line_number for tracking)
                    self.raw_lines.insert(i + 1, SourceLine(line.line_number, body_part))
                break

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

                part1 = ""
                part2 = ""

                if marker == "*/":
                    # Split AFTER the marker
                    split_point = idx + 2
                    part1 = content[:split_point]
                    part2 = content[split_point:]
                    # If part2 is empty? "lines ... shifted".
                    # If part2 is empty, do we make an empty line?
                    # Rule doesn't say "if rest is not empty".
                    # Let's assume strictly following rule.
                else:
                    # -- or /*
                    # Split BEFORE the marker
                    split_point = idx
                    part1 = content[:split_point]
                    part2 = content[split_point:]

                # Update current line
                self.body_lines[i].content = part1

                # Insert new line
                original_line_num = self.body_lines[i].line_number
                new_line_num = original_line_num + 1
                new_line = SourceLine(new_line_num, part2)

                self.body_lines.insert(i + 1, new_line)

                # Shift all following lines
                for j in range(i + 2, len(self.body_lines)):
                    self.body_lines[j].line_number += 1

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

    def pass_2_extract_comments(self):
        """
        Pass 2: Extracts comments from body lines.
        Removes comment lines from body lines.
        Stores comments in self.comments.
        """
        self.log("Running Pass 2: Extract Comments")

        new_body_lines = []
        in_comment_block = False
        current_comment_lines = []
        current_comment_start_line = -1

        for line in self.body_lines:
            content = line.content.strip()

            if in_comment_block:
                current_comment_lines.append(line.content)
                # Check for end of comment
                # Pass 2 ensures */ is at end of line (or at least split) if it was inline?
                # Does text containing '*/' end the block?
                if "*/" in content:
                    # Ends on same line
                    # check if */ is not quoted? Assuming comments don't care about quotes inside them?
                    # Rule 44: "Any key words which might be part of a comment are not parsed as key words"
                    # But finding the END of the comment?
                    # "end with the closest */ group"

                    # We just assume if "*/" is present, it closes it.
                    in_comment_block = False

                    # Rule: "remove all spaces ... keep new line characters"
                    cleaned_lines = [l.strip() for l in current_comment_lines]
                    full_comment_text = "\n".join(cleaned_lines)

                    self.comments.append({
                        "line": current_comment_start_line,
                        "content": full_comment_text
                    })
                    current_comment_lines = []
                continue

            # Not in comment block
            if content.startswith("/*"):
                in_comment_block = True
                current_comment_start_line = line.line_number
                current_comment_lines.append(line.content)

                if "*/" in content:
                    # Ends on same line
                    in_comment_block = False

                    # Rule: "remove all spaces ... keep new line characters"
                    cleaned_lines = [l.strip() for l in current_comment_lines]
                    full_comment_text = "\n".join(cleaned_lines)

                    self.comments.append({
                        "line": current_comment_start_line,
                        "content": full_comment_text
                    })
                    current_comment_lines = []
                continue

            if content.startswith("--"):
                # Single line comment
                # Rule 47: convert to /* ... */
                # Rule 48: remove previous */

                # Remove leading --
                raw_text = line.content
                # Check: line.content might have indentation?
                # "starts with --" -> from stripped content.
                # We want to preserve content?
                # Rule 47: "All comments starting with '--' must be encapsulated into '/*' and '*/'"
                # Example: "-- foo" -> "/* -- foo */" ? Or "/* foo */"?
                # Usually we wrap the whole thing.

                encapsulated = "/*" + raw_text + "*/"

                # Rule: "replace all patterns of '*/ */' with single '*/'" (New Pass 2 rule)
                # But wait, this applies to ALL comments at the end of the pass.
                # The "encapsulate --" part is during the pass.
                # I should just append the encapsulated comment here.
                # The rule saying "only the last */ group ... kept" was REMOVED/CHANGED.
                # So I DO NOT remove previous */ here explicitly?
                # The new rule says: "at the end of this step iterate ... replace '*/ */' ... with single '*/'".
                # So here I just encapsulate.

                self.comments.append({
                    "line": line.line_number,
                    "content": encapsulated
                })
                continue

            # If not a comment, keep the line
            new_body_lines.append(line)

        # End of loop.
        # "at the end of this step iterate through all lines of comments array..."
        # Rule: "replace pattern '*/ */' with single '*/'"

        for comment in self.comments:
            # Pattern: "*/" followed by ZERO, one or more space(s) followed by "*/"
            # Regex: \*/\s*\*/
            # Replace with: */
            comment['content'] = re.sub(r'\*/\s*\*/', '*/', comment['content'])

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
                # Start of declaration
                start_line = line.line_number
                decl_lines = []

                # Consume lines until end condition met
                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(decl_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE)
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
                        if re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE):
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
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                             # Check if we should CONTINUE anyway
                             prev_content = insert_lines[-1].strip()
                             should_continue = False

                             # Rule 67: prev ends with "," or "="
                             if prev_content.endswith(",") or prev_content.endswith("=") or prev_content.endswith("("):
                                 should_continue = True

                             # Rule 68: next line (current line) starts with "," or "="
                             if current_content.startswith(",") or current_content.startswith("="):
                                 should_continue = True

                             # Rule 69: next line (current line) starts with "FROM"
                             if re.match(r'^FROM\b', current_content, re.IGNORECASE):
                                 should_continue = True

                             # INSERT ... SELECT continuation
                             if re.match(r'^SELECT\b', current_content, re.IGNORECASE):
                                 has_values = any(re.search(r'\bVALUES\b', l, re.IGNORECASE) for l in insert_lines)
                                 has_select = any(re.search(r'\bSELECT\b', l, re.IGNORECASE) for l in insert_lines)
                                 if not has_values and not has_select:
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
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE)

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
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE)

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

                            if not should_continue:
                                break

                    delete_lines.append(current_line.content)
                    i += 1

                # Rule: "remove all spaces ... remove new line characters"
                cleaned_lines = [l.strip() for l in delete_lines]
                full_delete = " ".join(cleaned_lines)

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

                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE)

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
                    match = re.match(r'^((?:\'[^\']*\')|(?:"[^"]*"))(.*)$', print_args, re.IGNORECASE)
                    if match:
                        format_str_raw = match.group(1)
                        args = match.group(2).strip()
                        
                        if format_str_raw.startswith('"') and format_str_raw.endswith('"'):
                            # User requested to remove single quotes completely before replacing main double quotes
                            format_str = format_str_raw[1:-1].replace("'", "")
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

    def pass_5d_parse_sets(self):
        """
        Pass 5d: Parses SET commands that modify behavior (e.g., SET NOCOUNT ON).
        Skips SET @var and SET ROWCOUNT.
        Starts with SET.
        Encapsulates into /* ... - Sybase Syntax */.
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

            if is_set and not is_var and not is_rowcount:
                start_line = line.line_number
                set_lines = []

                while i < len(self.body_lines):
                    current_line = self.body_lines[i]
                    current_content = current_line.content.strip()

                    if len(set_lines) > 0:
                        is_terminator = re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE)

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
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE):
                            is_terminator = True

                        if is_terminator:
                            break

                    raiserror_lines.append(current_line.content)
                    i += 1

                cleaned_lines = [l.strip() for l in raiserror_lines]
                full_raiserror = " ".join(cleaned_lines)

                # Now parse RAISERROR <number> <string>
                match = re.match(r'^RAISERROR\s+(\d+)\s+((?:\'[^\']*\')|(?:"[^"]*"))(.*)$', full_raiserror, re.IGNORECASE)
                if match:
                    err_num = match.group(1)
                    err_msg_raw = match.group(2)
                    args = match.group(3).strip()

                    if (err_msg_raw.startswith('"') and err_msg_raw.endswith('"')) or (err_msg_raw.startswith("'") and err_msg_raw.endswith("'")):
                        err_msg = err_msg_raw[1:-1]
                    else:
                        err_msg = err_msg_raw

                    err_msg = err_msg.replace("'", "''")
                    err_msg = re.sub(r'%\d+!', '%', err_msg)
                    
                    if args:
                        args = args.lstrip(',').strip()
                        full_raiserror = f"RAISE EXCEPTION '{err_msg} %', {args}, {err_num}"
                    else:
                        full_raiserror = f"RAISE EXCEPTION '{err_msg} %', {err_num}"
                else:
                    # fallback
                    full_raiserror = re.sub(r'^RAISERROR\b', 'RAISE EXCEPTION', full_raiserror, flags=re.IGNORECASE)

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
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE):
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
                                     terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b'
                                     if not re.match(terminator_pattern, next_l_check, re.IGNORECASE):
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
                                terminator_pattern = r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b'
                                if re.match(terminator_pattern, next_l_check, re.IGNORECASE):
                                    is_terminator = True
                            else:
                                is_terminator = True
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', current_content, re.IGNORECASE):
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
        Pass 3b: Splits inline IF statements.
        Sybase allows `IF condition command`.
        This pass splits such lines into two: `IF condition` and `command`.
        """
        self.log("Running Pass 3b: Split Inline IFs")
        new_body_lines = []
        keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "PRINT", "EXEC", "EXECUTE", "BEGIN", "RETURN", "SET"]
        
        for line in self.body_lines:
            content = line.content.strip()
            if re.match(r'^(IF|ELSE\s+IF)\b', content, re.IGNORECASE):
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
                        elif re.match(r'^(IF|ELSE\s+IF|ELSE|ELSIF|END|UPDATE|INSERT|DELETE|RETURN|SELECT|PRINT|SET|BEGIN|EXEC|EXECUTE|WHILE|COMMIT|ROLLBACK|DECLARE|CREATE|ALTER|DROP|RAISERROR)\b', next_line_content, re.IGNORECASE):
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
            pass_check = False

            # Normalize spaces for check
            normalized = re.sub(r'\s+', ' ', original_content)

            # Regex: ^SELECT\s+@\w+\s*=\s*
            # Matches SELECT @... = ...
            if re.match(r'^SELECT\s+@[\w@]+\s*=', normalized, re.IGNORECASE):
                pass_check = True

            if pass_check:
                # Transform
                # 1. Remove SELECT
                # Case insensitive replace of first SELECT
                cleaned = re.sub(r'^SELECT\s+', '', normalized, count=1, flags=re.IGNORECASE)

                # 2. Replace , with ; outside of parens/quotes, but stop replacing when hitting a FROM clause
                cleaned = self.replace_commas_outside_parens(cleaned, stop_word="from")

                # 3. Replace = with := for the assignment exclusively (Rule 112)
                # Instead of a global .replace(), target only the assignment operator matching the @variable definition!
                cleaned = re.sub(r'(@[\w@]+)\s*=', r'\1 :=', cleaned, count=1)

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
            self.comments
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

    def pass_8d_convert_selects(self):
        """
        Pass 8d: Converts SELECT commands using the view converter if provided.
        """
        if not self.view_converter or not self.settings:
            return

        self.log("Running Pass 8d: Convert SELECTs with view converter")

        for cmd_obj in self.select_commands:
            original_content = cmd_obj['content']
            
            temp_settings = self.settings.copy()
            temp_settings['view_code'] = original_content
            
            try:
                converted = self.view_converter(temp_settings)
                
                if original_content.strip().endswith(';') and not converted.strip().endswith(';'):
                    converted += ';'
                
                cmd_obj['content'] = converted
            except Exception as e:
                self.log(f"Failed to convert SELECT command: {e}")


    def pass_9_rename_variables(self):
        """
        Pass 9: Global replacement of local variable notation.
        Iterate over all arrays and replace @var with locvar_var.
        Ignore @@var.
        Case-insensitive check, preserve case of var.
        """
        self.log("Running Pass 9: Rename Variables")

        def replacement_func(match):
            return "locvar_" + match.group(1)

        def apply_rename(content):
            # Regex: (?<!@)@([a-zA-Z0-9_]+)
            return re.sub(r'(?<!@)@([a-zA-Z0-9_]+)', replacement_func, content, flags=re.IGNORECASE)

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
            self.comments
        ]

        for array in targets:
            for item in array:
                item['content'] = apply_rename(item['content'])



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
                line.content = content + ";"
                continue

            # Rule 122: BEGIN -> keep unchanged
            if re.match(r'^BEGIN\b', content, re.IGNORECASE) and content.upper() == "BEGIN":
                continue

            # Allow SET ROWCOUNT to pass through cleanly so it can be handled by pass 11
            if re.match(r'^SET\s+ROWCOUNT\b', content, re.IGNORECASE):
                continue

            # Skip lines that are already comments
            if content.startswith('--') or content.startswith('/*'):
                continue

            # Rule 123: Anything else -> add TODO
            # Check if it was modified by above rules?

            # If we are here, it's a TODO line.
            line.content = line.content + "; /* TODO: not processed line - check syntax */"

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
        if self.variables:
            add_line("declare_section", 0, "DECLARE")

            # 4. Variables
            for var in self.variables:
                add_line("variable_declaration", var['line'], var['content'])

        # 5. Body Output Array
        # Collect all parts, sort by original line number, then append.

        # Structure: (line_number, content, source_name)
        body_parts = []

        for l in self.body_lines:
            body_parts.append((l.line_number, l.content, "remaining_body_lines"))

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
            # It's a comment, so we don't need a semicolon, but it doesn't hurt if we treat it uniformly or just skip adding it.
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
                    # Split into procedure name and arguments
                    parts = re.split(r'\s+', remainder, 1)
                    proc_name = parts[0]
                    args = parts[1].strip() if len(parts) > 1 else ""
                    content = f"PERFORM {proc_name}({args})"
                else:
                    # Keep as EXECUTE for dynamic SQL
                    content = f"EXECUTE {remainder}"

            if not content.strip().endswith(';'):
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
            m = re.match(r'^\s*SET\s+ROWCOUNT\s+(\d+)', content, re.IGNORECASE)
            if m:
                limit_val = m.group(1)
                active_rowcount_limit = None if limit_val == '0' else limit_val
                content = re.sub(r'(?i)^\s*SET\s+ROWCOUNT\s+\d+', f'/* SET ROWCOUNT {limit_val} converted to LIMIT */', content)

            # Apply LIMIT to SELECT commands
            elif source_name == "select_commands" and active_rowcount_limit:
                if not re.search(r'\bLIMIT\s+\d+', content, re.IGNORECASE):
                    if content.strip().endswith(';'):
                        content = content.strip()[:-1] + f" LIMIT {active_rowcount_limit};"
                    else:
                        content = content.strip() + f" LIMIT {active_rowcount_limit}"

            new_body_parts.append((line_num, content, source_name))

        body_parts = new_body_parts

        # Inject BEGIN and END if they are missing from the Sybase source
        injected_begin = False
        if body_parts:
            first_content = next((x[1].strip().upper() for x in body_parts if x[1].strip()), "")
            if first_content != "BEGIN":
                add_line("injected_begin", 0, "BEGIN")
                injected_begin = True

        # Append to output_array
        for item in body_parts:
            add_line(item[2], item[0], item[1])

        if body_parts:
            last_content = next((x[1].strip().upper() for x in reversed(body_parts) if x[1].strip()), "")
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
                if_stack.append({"state": "EXPECT_TARGET"})
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
                    if_stack.pop()
                    orig_line = getattr(line, 'original_line_number', getattr(line, 'line_number_approx', 0))
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

        # Pass 2
        self.pass_2_extract_comments()

        # Pass 3
        self.pass_3_parse_variables()

        # Pass 3b
        self.pass_3b_split_inline_ifs()

        # Pass 4
        self.pass_4_parse_inserts()

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

        # Pass 8d
        self.pass_8d_convert_selects()

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
            target_db_type = 'postgresql'
            if self.config_parser:
                target_conn = self.config_parser.get_connectivity('target')
                target_db_type = target_conn.get('db_type', 'postgresql') if target_conn else 'postgresql'
            for line in final_output:
                line.content = self.functions_mapping_converter(line.content, {'target_db_type': target_db_type})

        return final_output