"""Quick test for the regex fallback extraction functions in merger.py."""

from pipeline.merger import _extract_bed_count, _extract_doctor_count

# ── Test _extract_bed_count ──────────────────────────────────────────────────

tests_bed = [
    # (input_arrays, expected)
    ([["Maintains 15 wards with 300 operational beds"]], 300),
    ([["Has capacity to accommodate 600 patients"]], 600),
    ([["Overall bed capacity of 39 beds (VIP and common wards)"]], 39),
    ([["Operates 300 beds currently"]], 300),
    ([["Has 100-bed capacity"]], 100),
    ([["Has an on-site pharmacy"]], None),  # No bed info
    ([None, ["bed capacity of 50"]], 50),   # First array is None
    ([None], None),                          # All None
    ([[]], None),                             # Empty
]

print("=== _extract_bed_count tests ===")
all_pass = True
for i, (arrays, expected) in enumerate(tests_bed, 1):
    result = _extract_bed_count(arrays)
    status = "✓" if result == expected else "✗"
    if result != expected:
        all_pass = False
    print(f"  {status} Test {i}: input={arrays[0][:1] if arrays[0] else 'None'!r}... → got={result}, expected={expected}")

# ── Test _extract_doctor_count ───────────────────────────────────────────────

tests_doc = [
    ([["Has 5 medical doctors on staff"]], 5),
    ([["Employs 12 doctors"]], 12),
    ([["Has 3 physicians on site"]], 3),
    ([["Has specialized medical and nursing staff"]], None),  # No number
    ([None], None),
]

print("\n=== _extract_doctor_count tests ===")
for i, (arrays, expected) in enumerate(tests_doc, 1):
    result = _extract_doctor_count(arrays)
    status = "✓" if result == expected else "✗"
    if result != expected:
        all_pass = False
    print(f"  {status} Test {i}: got={result}, expected={expected}")

print(f"\n{'ALL TESTS PASSED' if all_pass else 'SOME TESTS FAILED'}")
