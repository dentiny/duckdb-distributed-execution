#!/usr/bin/env python3
"""Compare dh_q<N>.csv with ref_q<N>.csv as sorted rows, allowing float rounding from merge order."""
import csv
import math
import sys
from pathlib import Path


def rows(path):
    with open(path, newline="") as f:
        return sorted(csv.reader(f))


def same(left, right):
    if left == right:
        return True
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-9)
    except ValueError:
        return False


def main():
    out = Path(sys.argv[1])
    failed = []
    for q in range(1, 23):
        actual, expected = rows(out / f"dh_q{q}.csv"), rows(out / f"ref_q{q}.csv")
        ok = len(actual) == len(expected) and all(
            len(a) == len(e) and all(map(same, a, e)) for a, e in zip(actual, expected)
        )
        if not ok:
            print(f"Q{q:02d} MISMATCH ({len(actual)} vs {len(expected)} rows)")
            failed.append(q)
    print(f"verify: {22 - len(failed)}/22 queries match (results in {out})")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
