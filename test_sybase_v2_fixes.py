import sys
import os
import re
from unittest.mock import MagicMock

# Mock dependencies
sys.modules['jaydebeapi'] = MagicMock()
sys.modules['tabulate'] = MagicMock()

# Import after mocking
from credativ_pg_migrator.connectors.sybase_ase_connector import SybaseASEConnector

def test_v2_fixes():
    print("Testing V2 Fixes...")

    # Setup Connector
    mock_config = MagicMock()
    mock_config.get_log_file.return_value = '/dev/null'
    connector = SybaseASEConnector(mock_config, 'source')

    # Mock settings
    settings = {
        'target_db_type': 'postgresql',
        'target_schema': 'public',
        'funcproc_name': 'test_proc',
        'migrator_tables': MagicMock() # Mock protocol table
    }

    # Mock UDT mapping
    # We cheat a bit and inject into the connector's internal map cache if possible,
    # or ensure _get_udt_codes_mapping returns what we want.
    # Actually, simpler: define types_mapping to include the UDT for now or mock the method.
    connector.get_types_mapping = MagicMock(return_value={'int': 'integer', 'TypID': 'integer'})
    # Mock UDT substitution internals
    connector._apply_data_type_substitutions = MagicMock(side_effect=lambda x: x)
    connector._apply_udt_to_base_type_substitutions = MagicMock(side_effect=lambda x, y: x.replace('TypID', 'integer')) # Simulate replacement

    # Test Case 1: Double Parens Bug
    print("\n[TEST 1] Double Parentheses Logic")
    code_double_parens = "CREATE PROC cc_getVertragIdByDomainG2 ((@a int)) AS BEGIN SELECT 1 END"
    settings['funcproc_code'] = code_double_parens
    settings['funcproc_name'] = 'cc_getVertragIdByDomainG2'

    res1 = connector.convert_funcproc_code_v2(settings)
    print(f"Result 1:\n{res1}")

    if "((@a" in res1 or "((a" in res1:
        print("FAIL: Double parens detected!")
    elif "(a integer)" in res1:
        print("PASS: Double parens stripped.")
    else:
        print("WARN: Check output.")


    # Test Case 2: HEADER Parsing (Regex check)
    print("\n[TEST 2] Header Regex Check")
    # V2 regex had issues with some formats.

    # Test Case 3: OUTPUT Parameters
    print("\n[TEST 3] OUTPUT Params -> INOUT / RETURNS")
    code_output = "CREATE PROC test_out (@res int OUTPUT) AS BEGIN SELECT 1 END"
    settings['funcproc_code'] = code_output
    settings['funcproc_name'] = 'test_out'

    res3 = connector.convert_funcproc_code_v2(settings)
    print(f"Result 3:\n{res3}\n")

    if "INOUT res integer" in res3 and "RETURNS integer" in res3:
        print("PASS: OUTPUT converted to INOUT and RETURNS calculated.")
    else:
        print("FAIL: OUTPUT param handling incorrect.")

    # Test Case 4: RAISERROR
    print("\n[TEST 4] RAISERROR Conversion")
    code_raise = "CREATE PROC test_raise AS BEGIN RAISERROR 50000 'Error Message' END"
    settings['funcproc_code'] = code_raise
    settings['funcproc_name'] = 'test_raise'

    res4 = connector.convert_funcproc_code_v2(settings)
    print(f"Result 4 Body snippet:\n{res4}")

    if "RAISE EXCEPTION 'Error Message' USING ERRCODE = '50000'" in res4:
        print("PASS: RAISERROR converted.")
    else:
        print("FAIL: RAISERROR not converted.")

if __name__ == "__main__":
    test_v2_fixes()
