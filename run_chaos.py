"""
Run every scenario and bake the results into results.json.

The site is static: this script is the only thing that executes Raft. Its
output is what the report renders, so the site needs no backend at all.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from chaos.scenarios import ALL_SCENARIOS

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "site" / "results.json"


def main() -> int:
    results = []
    print(f"Running {len(ALL_SCENARIOS)} scenarios\n")

    for fn in ALL_SCENARIOS:
        result = fn()
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        check = result.check
        print(
            f"  [{status}] {result.name:<38} "
            f"ticks={result.total_ticks:<5} "
            f"acked={check['acked_writes']:<3} "
            f"msgs={result.counters['messages_sent']:<6} "
            f"dropped={result.counters['messages_dropped']:<5} "
            f"violations={len(check['violations'])}"
        )
        for violation in check["violations"][:5]:
            print(f"        ! {violation['kind']} @ tick {violation['tick']}: {violation['message']}")

    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    total_violations = sum(len(r.check["violations"]) for r in results)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine": {
            "repo": "https://github.com/abho7/raft-kv-store",
            "note": "Cloned read-only; driven externally. Not modified by this harness.",
        },
        "summary": {
            "scenarios": len(results),
            "passed": passed,
            "failed": failed,
            "violations": total_violations,
            "total_ticks": sum(r.total_ticks for r in results),
            "total_messages": sum(r.counters["messages_sent"] for r in results),
            "total_dropped": sum(r.counters["messages_dropped"] for r in results),
            "acked_writes": sum(r.check["acked_writes"] for r in results),
        },
        "scenarios": [r.to_json() for r in results],
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"\n{passed}/{len(results)} scenarios passed, {total_violations} violations")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
