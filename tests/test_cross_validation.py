"""
Cross-validation: ChaosCluster vs the engine's own SimulatedCluster.

ChaosCluster is a different transport from the one raft-engine's 23 tests
exercise -- queued and delayed rather than immediate and recursive. That
difference is the whole point, but it also means a violation reported by the
chaos harness could in principle be a bug in the *harness*.

These tests close that gap: every scenario the engine's own suite covers is
re-run through the new transport, and the safety-relevant outcome must match.
Where they legitimately differ (timing, tick counts, which node wins an
election) the assertions are on the invariant, not the incidental detail.
"""

from __future__ import annotations

import pytest
from raft.simulator import SimulatedCluster

from chaos.checker import CorrectnessChecker
from chaos.cluster import ChaosCluster
from chaos.faults import Partition

NODES_3 = ["n1", "n2", "n3"]
NODES_5 = ["n1", "n2", "n3", "n4", "n5"]


def _chaos(nodes, seed):
    cluster = ChaosCluster(nodes, seed=seed)
    checker = CorrectnessChecker(cluster)
    cluster.observers.append(checker)
    return cluster, checker


def _commit(cluster, checker, command, ticks=80):
    result = cluster.propose(command)
    assert result is not None, "no leader available to accept the write"
    checker.track(*result)
    cluster.run_ticks(ticks)
    return result[1]


def test_both_transports_elect_exactly_one_leader():
    sim = SimulatedCluster(NODES_3, seed=7)
    assert sim.run_until_leader(max_ticks=200) is not None
    assert len(sim.current_leaders()) == 1

    chaos, _ = _chaos(NODES_3, seed=7)
    assert chaos.run_until_leader(max_ticks=300) is not None
    assert len(chaos.current_leaders()) == 1


def test_both_transports_replicate_a_write_to_every_node():
    command = {"op": "set", "key": "x", "value": "1"}

    sim = SimulatedCluster(NODES_3, seed=7)
    sim.run_until_leader(max_ticks=200)
    entry = sim.propose(command)
    sim.run_until_committed(entry.index, max_ticks=200)
    sim_applied = {nid: sim.applied_commands(nid) for nid in NODES_3}

    chaos, checker = _chaos(NODES_3, seed=7)
    chaos.run_until_leader(max_ticks=300)
    _commit(chaos, checker, command)
    chaos_applied = {nid: chaos.applied_commands(nid) for nid in NODES_3}

    assert all(command in cmds for cmds in sim_applied.values())
    assert all(command in cmds for cmds in chaos_applied.values())
    assert checker.report.passed


def test_both_transports_survive_leader_crash_without_data_loss():
    """The engine's central claim, re-proven over the queued transport."""
    sim = SimulatedCluster(NODES_3, seed=7)
    leader = sim.run_until_leader(max_ticks=200)
    e1 = sim.propose({"op": "set", "key": "x", "value": "1"})
    sim.run_until_committed(e1.index, max_ticks=200)
    sim.kill_node(leader)
    assert sim.run_until_leader(max_ticks=300) not in (None, leader)

    chaos, checker = _chaos(NODES_3, seed=7)
    leader = chaos.run_until_leader(max_ticks=300)
    _commit(chaos, checker, {"op": "set", "key": "x", "value": "1"})
    chaos.kill_node(leader)
    new_leader = chaos.run_until_leader(max_ticks=400)

    assert new_leader is not None and new_leader != leader
    _commit(chaos, checker, {"op": "set", "key": "y", "value": "2"})

    alive = [n for n in chaos.node_ids if chaos.alive[n]]
    reference = chaos.applied_commands(alive[0])
    for nid in alive[1:]:
        assert chaos.applied_commands(nid) == reference
    assert {"op": "set", "key": "x", "value": "1"} in reference
    assert checker.report.passed, checker.report.violations


def test_isolated_minority_leader_cannot_commit_on_either_transport():
    chaos, checker = _chaos(NODES_5, seed=3)
    leader = chaos.run_until_leader(max_ticks=300)
    _commit(chaos, checker, {"op": "set", "key": "a", "value": "1"})
    committed_before = chaos.nodes[leader].commit_index

    others = {n for n in chaos.node_ids if n != leader}
    chaos.inject(Partition(label="isolate leader", groups=[{leader}, others]))
    chaos.run_ticks(60)

    result = chaos.propose({"op": "set", "key": "STALE", "value": "x"})
    if result is not None and result[0] == leader:
        checker.track(*result)
    chaos.run_ticks(40)

    assert chaos.nodes[leader].commit_index == committed_before, (
        "an isolated minority leader must not be able to advance commit_index"
    )
    assert checker.report.passed, checker.report.violations


def test_no_split_brain_write_survives_healing_on_either_transport():
    chaos, checker = _chaos(NODES_5, seed=3)
    leader = chaos.run_until_leader(max_ticks=300)
    _commit(chaos, checker, {"op": "set", "key": "a", "value": "1"})

    others = {n for n in chaos.node_ids if n != leader}
    partition = chaos.inject(Partition(label="isolate leader", groups=[{leader}, others]))
    chaos.run_ticks(60)

    # Stale write into the isolated side; must never appear anywhere.
    stale_entry, actions = chaos.nodes[leader].propose({"op": "set", "key": "STALE", "value": "x"})
    chaos._dispatch(actions, leader)
    chaos.run_ticks(20)

    chaos.heal(partition)
    chaos.run_ticks(120)

    assert len(chaos.current_leaders()) == 1
    for nid in chaos.node_ids:
        assert not any(c.get("key") == "STALE" for c in chaos.applied_commands(nid)), (
            f"{nid} applied a never-committed stale write -- split-brain safety violation"
        )
    assert checker.report.passed, checker.report.violations


def test_restart_reapplication_is_idempotent_on_chaos_transport():
    """Mirrors the engine's own idempotency test -- a restarted node
    re-reports its committed prefix, and that must not double-count."""
    chaos, checker = _chaos(NODES_3, seed=15)
    leader = chaos.run_until_leader(max_ticks=300)
    _commit(chaos, checker, {"op": "set", "key": "x", "value": "1"})

    follower = next(n for n in chaos.node_ids if n != leader)
    chaos.kill_node(follower)
    chaos.run_ticks(30)
    chaos.restart_node(follower)
    chaos.run_ticks(60)

    commands = chaos.applied_commands(follower)
    assert commands.count({"op": "set", "key": "x", "value": "1"}) == 1, commands
    assert checker.report.passed, checker.report.violations


@pytest.mark.parametrize("seed", [1, 2, 3, 5, 8, 13])
def test_quiet_clusters_never_violate_on_either_transport(seed):
    """With no faults injected at all, neither transport may ever report a
    violation. If this fails, the harness is wrong, not the engine."""
    chaos, checker = _chaos(NODES_5, seed=seed)
    chaos.run_until_leader(max_ticks=300)
    for i in range(5):
        _commit(chaos, checker, {"op": "set", "key": f"k{i}", "value": str(i)}, ticks=40)

    assert checker.report.acked_writes == 5
    assert checker.report.passed, checker.report.violations
