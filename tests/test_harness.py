"""
Tests for the chaos harness itself.

A chaos report is only as trustworthy as the harness producing it, and this
one produced three false signals during development before it produced a
single true one. These tests pin the mechanics -- that a delay really delays,
that a per-link drop really targets one link, that faults compose -- and pin
the false positive that got fixed.
"""

from __future__ import annotations

import pytest

from chaos.checker import CorrectnessChecker
from chaos.cluster import ChaosCluster
from chaos.faults import LinkFault, Partition
from chaos.fuzz import run_one

NODES = ["n1", "n2", "n3", "n4", "n5"]


def _cluster(seed=1, **kwargs):
    cluster = ChaosCluster(NODES, seed=seed, **kwargs)
    checker = CorrectnessChecker(cluster)
    cluster.observers.append(checker)
    return cluster, checker


# --------------------------------------------------------------- transport

def test_messages_are_queued_not_delivered_instantly():
    """The whole reason this transport exists: a message sent on tick T must
    not be handled during tick T, or 'delay' has nowhere to live."""
    cluster, _ = _cluster()
    cluster.tick()
    assert cluster.log.messages_sent >= 0
    # After a single tick nothing can have round-tripped, so no node can have
    # collected a full majority of votes yet.
    assert all(n.role.value != "leader" for n in cluster.nodes.values())


def test_link_delay_defers_delivery_by_the_configured_ticks():
    cluster, _ = _cluster()
    cluster.inject(LinkFault(label="slow", sender="n1", recipient="n2", delay_ticks=10))
    cluster.nodes["n1"]  # sender exists
    before = cluster.log.messages_delayed
    cluster._enqueue("n1", "n2", object())
    assert cluster.log.messages_delayed == before + 1
    queued = cluster._queue[-1]
    assert queued.deliver_at == cluster.current_tick + 1 + 10


def test_link_fault_targets_only_the_named_link():
    cluster, _ = _cluster()
    cluster.inject(LinkFault(label="lossy", sender="n1", recipient="n2", drop_prob=1.0))

    cluster._enqueue("n1", "n2", object())
    assert cluster._queue == [], "the targeted link should have dropped this"

    cluster._enqueue("n1", "n3", object())
    assert len(cluster._queue) == 1, "an untargeted link must be unaffected"


def test_bidirectional_link_fault_covers_the_reverse_direction():
    cluster, _ = _cluster()
    cluster.inject(LinkFault(label="both", sender="n1", recipient="n2",
                             drop_prob=1.0, bidirectional=True))
    cluster._enqueue("n2", "n1", object())
    assert cluster._queue == []


def test_partition_blocks_across_groups_only():
    cluster, _ = _cluster()
    cluster.inject(Partition(label="split", groups=[{"n1", "n2"}, {"n3", "n4", "n5"}]))
    assert not cluster._reachable("n1", "n3")
    assert not cluster._reachable("n3", "n1")
    assert cluster._reachable("n1", "n2")
    assert cluster._reachable("n3", "n4")


def test_faults_compose_rather_than_replace():
    """Two partitions and a link fault active at once, which the engine's own
    single-tuple partition field cannot represent."""
    cluster, _ = _cluster()
    cluster.inject(Partition(label="a", groups=[{"n1"}, {"n2", "n3", "n4", "n5"}]))
    cluster.inject(Partition(label="b", groups=[{"n2"}, {"n1", "n3", "n4", "n5"}]))
    cluster.inject(LinkFault(label="c", sender="n3", recipient="n4", delay_ticks=5))

    assert len(cluster.faults) == 3
    assert not cluster._reachable("n1", "n3")   # first partition
    assert not cluster._reachable("n2", "n3")   # second partition
    assert cluster._reachable("n3", "n4")       # delayed, but not blocked
    assert cluster._link_delay("n3", "n4") == 1 + 5


def test_partitions_with_disjoint_groups_are_required():
    with pytest.raises(ValueError):
        Partition(label="bad", groups=[{"n1", "n2"}, {"n2", "n3"}])


def test_healing_a_fault_restores_reachability():
    cluster, _ = _cluster()
    fault = cluster.inject(Partition(label="split", groups=[{"n1"}, {"n2", "n3", "n4", "n5"}]))
    assert not cluster._reachable("n1", "n2")
    cluster.heal(fault)
    assert cluster._reachable("n1", "n2")


def test_dead_nodes_are_unreachable_in_both_directions():
    cluster, _ = _cluster()
    cluster.kill_node("n3")
    assert not cluster._reachable("n1", "n3")
    assert not cluster._reachable("n3", "n1")


# ---------------------------------------------------------------- checker

def test_propose_to_refuses_a_node_that_is_not_leader():
    cluster, _ = _cluster()
    cluster.run_until_leader(300)
    follower = next(n for n in NODES if cluster.nodes[n].role.value != "leader")
    assert cluster.propose_to(follower, {"op": "set", "key": "x", "value": "1"}) is None


def test_uncommitted_entry_overwritten_is_not_counted_as_a_lost_ack():
    """Raft permits an uncommitted entry to disappear. That must be recorded
    as an overwrite, never as a durability violation."""
    cluster, checker = _cluster(seed=7)
    leader = cluster.run_until_leader(300)
    result = cluster.propose({"op": "set", "key": "doomed", "value": "1"})
    assert result is not None
    checker.track(*result)
    cluster.kill_node(leader)
    cluster.run_ticks(400)

    assert checker.report.passed, checker.report.violations


def test_seed_124_does_not_report_a_false_acked_read_violation():
    """Regression for a harness bug, not an engine bug.

    Under seed 124 an entry whose proposer was partitioned is acknowledged
    *after* a higher index has already been acknowledged for the same key.
    The checker used to record the newest ack by detection order, which
    regressed the expected value to one that a later write had legitimately
    superseded, and reported 50 violations against entirely correct Raft
    behaviour. The expected value is now tracked by log index.
    """
    outcome = run_one(124)
    assert outcome.passed, outcome.violations


@pytest.mark.parametrize("seed", range(40))
def test_randomized_fault_schedules_find_no_violations(seed):
    outcome = run_one(seed)
    assert outcome.passed, f"seed {seed}: {outcome.violations}"
