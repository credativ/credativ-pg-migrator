
import re
import time

def slow_way(text, udts):
    print("Slow way start")
    start = time.time()
    for udt, repl in udts.items():
        pattern = re.compile(rf'(?:\[|")?\b{re.escape(udt)}\b(?:\]|")?', flags=re.IGNORECASE)
        text = pattern.sub(repl, text)
    end = time.time()
    print(f"Slow way: {end-start:.4f}s")
    return text

def fast_way(text, udts):
    print("Fast way start")
    start = time.time()
    # Sort by length descending to handle prefix matches correctly (if any)
    sorted_udts = sorted(udts.keys(), key=len, reverse=True)
    
    # Build Regex (Handle boundaries and quoting)
    # The original regex was: (?:\[|")?\bUDT\b(?:\]|")?
    # This implies we match "UDT" or [UDT] or UDT
    # And replace with REPL.
    # Note: If we match [UDT], we replace with REPL?
    # Original logic: pattern.sub(final_def, text) -> Replaces the WHOLE match with REPL.
    # So [UDT] -> REPL. "UDT" -> REPL.
    
    # We can capture the pattern:
    escaped = [re.escape(u) for u in sorted_udts]
    pattern_str = r'(?:\[|")?\b(' + '|'.join(escaped) + r')\b(?:\]|")?'
    
    regex = re.compile(pattern_str, flags=re.IGNORECASE)
    
    def replacer(match):
        # We need to look up the UDT name matched.
        # But wait, match.group(0) might contain quotes/brackets.
        # match.group(1) captures the UDT name IF parentheses are around the alternation.
        # Python re: (A|B) captures in group 1.
        
        # We need to find WHICH key matched.
        # Since we use case-insensitive, we need to map lower(key) -> repl?
        # But we iterate dict.
        
        # Actually, simpler:
        # Just use the matched text to normalize (strip quotes) and lookup?
        full_match = match.group(0)
        core_match = match.group(1) # The UDT name inside
        
        # We need to find the specific rule that matched.
        # Since we have the core name, we can lookup in lower-case map (since IGNORECASE).
        # But UDT map keys case might vary?
        # Assuming keys are unique case-insensitively?
        
        # Optimization: Map udt_upper -> repl
        return udt_lookup.get(core_match.upper(), full_match)

    # Precompute lookup
    udt_lookup = {k.upper(): v for k, v in udts.items()}
    
    res = regex.sub(replacer, text)
    end = time.time()
    print(f"Fast way: {end-start:.4f}s")
    return res

# Test
udts = {f'TypUser{i}': 'VARCHAR(100)' for i in range(1000)}
text = "DECLARE @v1 TypUser1; DECLARE @v2 [TypUser999]; \n" * 5000 # 5000 lines

s_res = slow_way(text, udts)
f_res = fast_way(text, udts)

assert s_res == f_res
print("Results match")
