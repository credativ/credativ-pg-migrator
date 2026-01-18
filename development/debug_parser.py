
import sqlglot
from sqlglot import exp, TokenType
from sqlglot.dialects import TSQL
import sys

# Minimal CustomTSQL reproduction
class CustomTSQL(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        COMMANDS = TSQL.Tokenizer.COMMANDS - {TokenType.COMMAND, TokenType.SET}

    class Parser(TSQL.Parser):
        def parse(self, tokens, encoding=None):
             print(f"DEBUG: Parser.parse called with {len(tokens)} tokens")
             return super().parse(tokens, encoding)

        def _parse_alias(self, this, explicit=False):
             if self._curr.token_type in (TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.MERGE, TokenType.SET, TokenType.ELSE):
                  return this
             return super()._parse_alias(this, explicit)

        def _parse_command_custom(self):
            # sys.stderr.write(f"DEBUG: Entering _parse_command_custom for {self._curr.text}\n")
            if not self._curr: return None
            
            prev_is_print = self._prev.text.upper() == 'PRINT' if self._prev else False
            curr_is_print = self._curr.text.upper() == 'PRINT' if self._curr else False

            if curr_is_print:
                 self._advance()
            elif not prev_is_print:
                 if self._prev.text.upper() == 'SET':
                      expressions = []
                      balance = 0
                      while self._curr:
                           if self._curr.token_type in (TokenType.SEMICOLON, TokenType.END):
                                break
                           if balance == 0:
                                txt = self._curr.text.upper()
                                if txt in ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BEGIN', 'IF', 'WHILE', 'RETURN', 'DECLARE', 'CREATE', 'TRUNCATE', 'GO', 'ELSE'):
                                     break
                           if self._curr.token_type == TokenType.L_PAREN:
                                balance += 1
                           elif self._curr.token_type == TokenType.R_PAREN:
                                balance -= 1
                           expressions.append(self._curr.text)
                           self._advance()
                      res = " ".join(expressions)
                      return exp.Command(this='SET', expression=exp.Literal.string(res))
                 return self._parse_command()
            return exp.Command(this='PRINT', expression=self._parse_conjunction())

        STATEMENT_PARSERS = TSQL.Parser.STATEMENT_PARSERS.copy()
        STATEMENT_PARSERS[TokenType.COMMAND] = _parse_command_custom
        STATEMENT_PARSERS[TokenType.SET] = _parse_command_custom

        def _parse_if(self):
            # print("DEBUG: Inside _parse_if")
            condition = self._parse_conjunction()
            # print(f"DEBUG: Parsed Condition: {condition}")
            
            true_stmt = self._parse_statement()
            # print(f"DEBUG: Parsed True Stmt: {true_stmt}")
            
            while self._match(TokenType.SEMICOLON): pass 
            
            # DEBUG INSPECTION
            print(f"DEBUG: Checking ELSE. Curr: {self._curr.text if self._curr else 'None'}")
            print(f"DEBUG: Parser tokens len: {len(self._tokens)}")
            
            matched_else = False
            if self._match(TokenType.ELSE):
                 matched_else = True
            elif self._curr and self._curr.text.upper() == 'ELSE':
                 self._advance()
                 matched_else = True
            
            false_stmt = self._parse_statement() if matched_else else None
            return self.expression(exp.If, this=condition, true=true_stmt, false=false_stmt)

        if hasattr(TokenType, 'IF'): STATEMENT_PARSERS[getattr(TokenType, 'IF')] = lambda self: self._parse_if()

# Modified body_content: REMOVED SEMICOLON before ELSE line 2
body_content = """SET locvar_OptIn = 1; SET locvar_OptOut = 2; SET locvar_AusdruecklicheEinwilligungTelefonisch = '10'; SET locvar_AusdruecklicheEinwilligungOnline = '11'; SET locvar_MutmasslicheEinwilligung = '20'; SET locvar_Firmenkunde = '30'; SET locvar_Unbekannt = '40'; SET locvar_KeineEinwilligung = '50'; SET locvar_AusdruecklicheAblehnung = '60';SET locvar_permissionStatus = null;if(locvar_propertyTypeId in (77,79,82,85,100,102,110,111)) /* OptOut only*/
        SET locvar_permissionStatus = locvar_OptOut else if(locvar_propertyTypeId in (117,118,119,120)) /* OptIn or OptOut*/
        begin
            if(locvar_propertyValue in (locvar_AusdruecklicheEinwilligungTelefonisch, locvar_AusdruecklicheEinwilligungOnline)) /*OptIn*/
                SET locvar_permissionStatus = locvar_OptIn else if (locvar_propertyValue = locvar_AusdruecklicheAblehnung) /* OptOut*/
                SET locvar_permissionStatus = locvar_OptOut end
    else if(locvar_propertyTypeId = 115 and SUBSTRING(locvar_propertyValue,8,1) = '1') /* Vertriebskontaktstatus Freenet*/
        SET locvar_permissionStatus = locvar_OptOut else if(locvar_propertyTypeId in (132, 133, 134))
        SET locvar_permissionStatus = null"""

print(f"Input length: {len(body_content)}")
tokens = CustomTSQL.Tokenizer().tokenize(body_content)
print(f"Manual Token count: {len(tokens)}")

print("\n--- Testing sqlglot.parse Helper (Semicolons Removed) ---")
try:
    parsed = sqlglot.parse(body_content, read=CustomTSQL)
    print("sqlglot.parse: Parsed successfully!")
    for p in parsed:
        print(p.sql())
except Exception as e:
    print(f"sqlglot.parse Error: {e}")
    # import traceback
    # traceback.print_exc()
