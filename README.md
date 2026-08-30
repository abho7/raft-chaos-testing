# Raft chaos testing

[![ci](https://github.com/abho7/raft-chaos-testing/actions/workflows/pages.yml/badge.svg)](https://github.com/abho7/raft-chaos-testing/actions/workflows/pages.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Systematic fault injection and continuous correctness verification against
[abho7/raft-kv-store](https://github.com/abho7/raft-kv-store).

The engine is cloned into `./raft-engine` as a **read-only dependency**. Nothing
here modifies it; the harness imports `RaftNode` and drives it externally.

---

## Headline result

Six hand-built scenarios plus a 600-seed randomized sweep, covering **1,552
acknowledged writes**, found **no safety violation in the engine** — no lost
acknowledged write, no divergence at a log index, no term with two leaders.

Every defect this exercise surfaced was in the **harness**. Two of them would
have produced a confidently wrong bug report, so they are documented on the
report rather than quietly fixed. See *Findings* on the site.

That is a negative result and its limits are worth stating: it means no
violation was found by these faults, not that none exists. The engine also
runs without disk persistence, so a real process crash losing its term and vote
is outside what this harness models.

---

## Why a third transport

The engine already ships two: `raft/simulator.py` (in-memory) and
`kvstore/server.py` (asyncio TCP). Neither can express two of the required
faults:

| Fault | `SimulatedCluster` | `ChaosCluster` |
|---|---|---|
| Kill node mid-write | yes | yes |
| Partition into groups | two groups, one at a time | any number, several at once |
| Drop specific messages | global uniform `drop_rate` only | per directed link |
| **Delay messages** | **structurally impossible** | yes |
| Message counting | no | yes |

`SimulatedCluster._deliver` calls the recipient's handler synchronously and
recurses, so a message has no in-flight state and "delay by five ticks" has
nowhere to live. `node.py`'s own docstring anticipates the fix — a harness may
"deliver it, delay it, or drop it" — so `chaos/cluster.py` adds a message queue
and uses `RaftNode` unmodified.

Because that is a different code path from the engine's own 23 tests,
`tests/test_cross_validation.py` re-runs every scenario the engine's suite
covers through the new transport and asserts the safety-relevant outcomes
match. That is what makes a violation attributable to the engine rather than to
the harness.

---

## What is verified

Four properties, checked on **every tick** rather than at the end, so the report
can say *when* something broke:

1. **Election safety** — at most one leader per term.
2. **State machine safety** — no two nodes apply different commands at the same log index.
3. **Leader completeness** — every acknowledged entry is present, unchanged, in the log of every later leader.
4. **Acknowledged read consistency** — once the leader has applied up to an acknowledged write, reading that key from the leader returns that value.

A write is **acknowledged** only once the leader's `commit_index` covers it —
the moment a real server would answer the client. An uncommitted entry
disappearing is correct Raft behaviour, recorded as an overwrite, never a loss.

**Not** treated as violations: stale reads from followers. `ARCHITECTURE.md` §5
documents that reads are served locally with no read-index or leader lease, so
a lagging follower returning an old value is designed behaviour. Counting those
as failures would bury real signal in known limitations.

---

## Layout

```
raft-chaos-testing/
├── chaos/
│   ├── cluster.py     ChaosCluster: queued transport over RaftNode
│   ├── faults.py      Partition / LinkFault, composable per-message
│   ├── checker.py     the four properties, checked per tick
│   ├── scenarios.py   the six named scenarios
│   ├── events.py      structured event stream behind the timelines
│   └── fuzz.py        randomized fault schedules across seeds
├── tests/
│   ├── test_cross_validation.py   chaos transport vs the engine's simulator
│   └── test_harness.py            harness mechanics + pinned false positive
├── run_chaos.py       runs scenarios -> site/results.json
├── build_site.py      results.json -> self-contained site/index.html
└── raft-engine/       read-only clone (gitignored)
```

## Running it

```bash
git clone https://github.com/abho7/raft-kv-store.git ./raft-engine
python -m venv .venv && .venv\Scripts\activate    # source .venv/bin/activate elsewhere
pip install pytest

pytest                      # 64 harness tests
python -m chaos.fuzz        # randomized sweep
python run_chaos.py         # scenarios -> site/results.json
python build_site.py        # -> site/index.html
```

`site/index.html` inlines its data, so it opens correctly from the filesystem
as well as over HTTP.

## Deployment

`.github/workflows/pages.yml` clones the engine, runs both test suites, executes
the scenarios and publishes `site/` to GitHub Pages. The results are baked at
build time — there is no backend.
