"""
The fault-injection scenarios.

Each scenario is a plain function that drives a ChaosCluster and returns a
ScenarioResult carrying everything the report needs: the event stream, the
derived timeline, message counters, and the correctness verdict.

Faults compose because they are independent objects consulted per-message
(see faults.py), so "crash a node during an active partition" is not a
special case -- it is two faults being active at once.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from chaos.checker import CorrectnessChecker
from chaos.cluster import ChaosCluster
from chaos.events import EventKind
from chaos.faults import LinkFault, Partition


@dataclass
class ScenarioResult:
    name: str
    slug: str
    description: str
    fault_summary: str
    nodes: list[str]
    seed: int
    total_ticks: int
    passed: bool
    check: dict
    counters: dict
    events: list[dict]
    leader_intervals: list[dict] = field(default_factory=list)
    fault_windows: list[dict] = field(default_factory=list)
    writes: list[dict] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "slug": self.slug,
            "description": self.description,
            "fault_summary": self.fault_summary,
            "nodes": self.nodes,
            "seed": self.seed,
            "total_ticks": self.total_ticks,
            "passed": self.passed,
            "check": self.check,
            "counters": self.counters,
            "leader_intervals": self.leader_intervals,
            "fault_windows": self.fault_windows,
            "writes": self.writes,
            "events": self.events,
        }


def _derive_leader_intervals(events: list[dict], total_ticks: int) -> list[dict]:
    """Turn discrete role changes into [start, end) leadership bands."""
    intervals: list[dict] = []
    open_by_node: dict[str, dict] = {}

    for event in events:
        if event["kind"] == EventKind.LEADER_ELECTED.value:
            node = event["detail"]["node"]
            if node in open_by_node:
                open_by_node[node]["end"] = event["tick"]
                intervals.append(open_by_node.pop(node))
            open_by_node[node] = {
                "node": node,
                "term": event["detail"]["term"],
                "start": event["tick"],
                "end": None,
            }
        elif event["kind"] == EventKind.ROLE_CHANGE.value:
            node = event["detail"]["node"]
            if event["detail"]["role"] != "leader" and node in open_by_node:
                open_by_node[node]["end"] = event["tick"]
                intervals.append(open_by_node.pop(node))

    for interval in open_by_node.values():
        interval["end"] = total_ticks
        intervals.append(interval)

    return sorted(intervals, key=lambda i: (i["start"], i["node"]))


def _derive_fault_windows(events: list[dict], total_ticks: int) -> list[dict]:
    windows: list[dict] = []
    open_windows: list[dict] = []

    for event in events:
        detail = event["detail"]
        if event["kind"] == EventKind.FAULT_INJECTED.value:
            open_windows.append({
                "label": detail.get("label") or detail.get("kind", "fault"),
                "kind": detail.get("kind", "fault"),
                "node": detail.get("node"),
                "groups": detail.get("groups"),
                "start": event["tick"],
                "end": None,
                "detail": detail,
            })
        elif event["kind"] == EventKind.FAULT_HEALED.value:
            label = detail.get("label") or detail.get("kind")
            node = detail.get("node")
            for window in open_windows:
                if window["end"] is None and (window["label"] == label or window["node"] == node):
                    window["end"] = event["tick"]
                    break

    for window in open_windows:
        if window["end"] is None:
            window["end"] = total_ticks
        windows.append(window)
    return sorted(windows, key=lambda w: w["start"])


def _derive_writes(events: list[dict]) -> list[dict]:
    """Every proposal, in order.

    Deliberately a list rather than a dict keyed by log index: when a leader
    is unseated mid-write, a later leader reuses that index for a different
    command, and keying by index would overwrite the record of the very event
    the scenario exists to show. Matching an ack to its proposal therefore
    keys on (index, term), which is unique.
    """
    writes: list[dict] = []
    by_index_term: dict[tuple[int, int], dict] = {}

    for event in events:
        detail = event["detail"]
        key = (detail.get("index"), detail.get("term"))
        if event["kind"] == EventKind.WRITE_PROPOSED.value:
            row = {
                "index": detail["index"],
                "term": detail["term"],
                "command": detail["command"],
                "leader": detail.get("leader"),
                "proposed_tick": event["tick"],
                "acked_tick": None,
                "status": "pending",
            }
            writes.append(row)
            by_index_term[key] = row
        elif event["kind"] == EventKind.WRITE_ACKED.value:
            row = by_index_term.get(key)
            if row is not None:
                row["acked_tick"] = event["tick"]
                row["status"] = "acked"
        elif event["kind"] == EventKind.WRITE_LOST.value:
            row = by_index_term.get(key)
            if row is not None and row["status"] == "pending":
                row["status"] = "overwritten"

    return writes


def build_result(name, slug, description, fault_summary, cluster, checker) -> ScenarioResult:
    events = cluster.log.to_json()
    return ScenarioResult(
        name=name,
        slug=slug,
        description=description,
        fault_summary=fault_summary,
        nodes=cluster.node_ids,
        seed=getattr(cluster, "_seed_value", 0),
        total_ticks=cluster.current_tick,
        passed=checker.report.passed,
        check=checker.report.to_json(),
        counters=cluster.log.counters(),
        events=events,
        leader_intervals=_derive_leader_intervals(events, cluster.current_tick),
        fault_windows=_derive_fault_windows(events, cluster.current_tick),
        writes=_derive_writes(events),
    )


def _new(nodes, seed, **kwargs):
    cluster = ChaosCluster(nodes, seed=seed, **kwargs)
    cluster._seed_value = seed
    checker = CorrectnessChecker(cluster)
    cluster.observers.append(checker)
    return cluster, checker


def _write(cluster, checker, key, value):
    result = cluster.propose({"op": "set", "key": key, "value": value})
    if result is not None:
        checker.track(*result)
    return result


def _write_to(cluster, checker, node_id, key, value):
    """Direct a write at one specific node -- used whenever a scenario needs
    to say which side of a partition it is writing to."""
    if node_id is None:
        return None
    result = cluster.propose_to(node_id, {"op": "set", "key": key, "value": value})
    if result is not None:
        checker.track(*result)
    return result


# --------------------------------------------------------------- scenarios

def scenario_kill_leader_mid_write(seed: int = 7) -> ScenarioResult:
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    cluster.run_until_leader(300)

    _write(cluster, checker, "a", "1")
    cluster.run_ticks(30)

    # Propose, then kill the leader before replication can finish. The entry
    # is in exactly one log at this instant, so it must NOT survive as
    # committed -- but anything already acked must.
    leader = cluster.leader()
    _write(cluster, checker, "b", "2")
    cluster.kill_node(leader)

    cluster.run_until_leader(400)
    _write(cluster, checker, "c", "3")
    cluster.run_ticks(120)

    return build_result(
        "Leader killed mid-write",
        "kill-leader-mid-write",
        "A write is proposed and the leader is crashed in the same tick, before "
        "replication completes. Acknowledged writes must survive; the in-flight "
        "one is permitted to vanish.",
        "NodeCrash on the current leader, one tick after a proposal",
        cluster, checker,
    )


def scenario_partition_two_groups(seed: int = 3) -> ScenarioResult:
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    leader = cluster.run_until_leader(300)
    _write(cluster, checker, "a", "1")
    cluster.run_ticks(30)

    minority = {leader}
    majority = set(nodes) - minority
    partition = cluster.inject(Partition(label="leader isolated", groups=[minority, majority]))
    cluster.run_ticks(60)

    # Explicitly address each side. Left to `propose()`, the write would go to
    # whichever leader sorts first by node id -- which is the isolated one
    # here, making the scenario silently test the opposite of its claim.
    majority_leader = cluster.leader_within(majority)
    _write_to(cluster, checker, majority_leader, "b", "2")
    cluster.run_ticks(60)

    # And a write into the isolated minority, which must never commit.
    _write_to(cluster, checker, leader, "STALE", "x")
    cluster.run_ticks(30)

    cluster.heal(partition)
    cluster.run_ticks(120)

    return build_result(
        "Network partition, two groups",
        "partition-two-groups",
        "The cluster splits into a one-node minority holding the old leader and a "
        "four-node majority. The majority must elect a new leader and keep "
        "committing. A write aimed at the isolated leader must never commit, and "
        "must not survive the partition healing.",
        "Partition: {old leader} | rest of cluster, healed after 150 ticks",
        cluster, checker,
    )


def scenario_delayed_and_dropped_links(seed: int = 11) -> ScenarioResult:
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    cluster.run_until_leader(300)

    # Degrade two specific links rather than the whole network -- the
    # capability the engine's global drop_rate cannot express.
    slow = cluster.inject(LinkFault(label="n1->n3 slow", sender="n1", recipient="n3",
                                    delay_ticks=6, bidirectional=True))
    lossy = cluster.inject(LinkFault(label="n2->n4 lossy", sender="n2", recipient="n4",
                                     drop_prob=0.5, bidirectional=True))

    for i in range(6):
        _write(cluster, checker, f"k{i}", str(i))
        cluster.run_ticks(25)

    cluster.heal(slow)
    cluster.heal(lossy)
    cluster.run_ticks(120)

    return build_result(
        "Delayed and dropped links",
        "delayed-dropped-links",
        "Two specific links are degraded while writes continue: one gains six "
        "ticks of latency, the other drops half its messages. Consensus must "
        "still converge and every acknowledged write must survive.",
        "LinkFault n1<->n3 delay 6 ticks; LinkFault n2<->n4 drop 50%",
        cluster, checker,
    )


def scenario_election_storm(seed: int = 23) -> ScenarioResult:
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    cluster.run_until_leader(300)

    # Repeatedly unseat whoever is leading, while writes keep arriving.
    for round_index in range(4):
        _write(cluster, checker, f"s{round_index}", str(round_index))
        cluster.run_ticks(20)
        leader = cluster.leader()
        if leader is not None:
            isolate = cluster.inject(
                Partition(label=f"unseat {leader} (round {round_index + 1})",
                          groups=[{leader}, set(nodes) - {leader}])
            )
            cluster.run_ticks(45)
            cluster.heal(isolate)
            cluster.run_ticks(35)

    _write(cluster, checker, "final", "done")
    cluster.run_ticks(150)

    return build_result(
        "Repeated elections under write load",
        "election-storm",
        "Whoever holds leadership is isolated and released four times in a row "
        "while writes keep arriving, forcing repeated elections. Term numbers "
        "should climb steadily and no acknowledged write may be lost across any "
        "leadership handover.",
        "Four successive Partition faults, each isolating the sitting leader",
        cluster, checker,
    )


def scenario_crash_during_partition(seed: int = 31) -> ScenarioResult:
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    leader = cluster.run_until_leader(300)
    _write(cluster, checker, "a", "1")
    cluster.run_ticks(30)

    minority = {leader}
    majority = set(nodes) - minority
    partition = cluster.inject(Partition(label="leader isolated", groups=[minority, majority]))
    cluster.run_ticks(60)

    majority_leader = cluster.leader_within(majority)
    _write_to(cluster, checker, majority_leader, "b", "2")
    cluster.run_ticks(40)

    # Combined failure: crash a node inside the majority side WHILE the
    # partition is still active. The reachable set drops from 4 nodes to 3 of
    # 5 -- still a quorum, but only just.
    victim = next(n for n in majority if n != majority_leader)
    cluster.kill_node(victim)
    cluster.run_ticks(60)

    _write_to(cluster, checker, cluster.leader_within(majority - {victim}), "c", "3")
    cluster.run_ticks(60)

    cluster.heal(partition)
    cluster.restart_node(victim)
    cluster.run_ticks(150)

    return build_result(
        "Crash during an active partition",
        "crash-during-partition",
        "A node is crashed inside the majority side while the network is already "
        "split, taking the reachable set to exactly a bare quorum. Both faults "
        "are then healed together.",
        "Partition {leader} | rest, then NodeCrash inside the majority, both healed",
        cluster, checker,
    )


def scenario_bare_quorum_loss(seed: int = 47) -> ScenarioResult:
    """Drives the cluster below quorum, which must stall rather than diverge.

    Losing availability here is correct -- Raft trades liveness for safety.
    The check is that nothing is silently committed while no quorum exists.
    """
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    cluster, checker = _new(nodes, seed)
    cluster.run_until_leader(300)
    _write(cluster, checker, "a", "1")
    cluster.run_ticks(40)

    alive = list(nodes)
    for victim in alive[:3]:  # 5 -> 2 nodes: quorum is impossible
        cluster.kill_node(victim)
        cluster.run_ticks(30)

    _write(cluster, checker, "impossible", "x")
    cluster.run_ticks(80)

    for victim in alive[:3]:
        cluster.restart_node(victim)
    cluster.run_ticks(200)

    return build_result(
        "Quorum loss and recovery",
        "quorum-loss",
        "Three of five nodes are crashed, making a quorum impossible. A write "
        "issued during that window must NOT be acknowledged while it lasts -- it "
        "may only commit once enough nodes return. Unavailability here is correct "
        "behaviour, not a failure; the timeline below shows the write sitting "
        "unacknowledged until the restarts land.",
        "Three sequential NodeCrash faults, then all three restarted",
        cluster, checker,
    )


ALL_SCENARIOS: list[Callable[..., ScenarioResult]] = [
    scenario_kill_leader_mid_write,
    scenario_partition_two_groups,
    scenario_delayed_and_dropped_links,
    scenario_election_storm,
    scenario_crash_during_partition,
    scenario_bare_quorum_loss,
]
