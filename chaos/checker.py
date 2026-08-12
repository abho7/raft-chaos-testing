"""
Continuous correctness monitoring.

Runs as a per-tick observer rather than an end-of-run assertion, so a report
can say *when* something broke, not merely that the final state looked fine.
A violation that appears and then heals is still a violation, and an
end-of-run check would miss it entirely.

Four properties are checked. All four are things Raft actually promises, so a
failure here is a real bug rather than a documented limitation:

  1. ELECTION SAFETY -- at most one leader per term.
  2. STATE MACHINE SAFETY -- no two nodes ever apply different commands at
     the same log index.
  3. LEADER COMPLETENESS -- every acknowledged entry is present, unchanged,
     in the log of every leader elected afterwards. This is the race-free way
     to express "an acked write is never lost": Raft guarantees a new leader
     *has* every committed entry, so it can be asserted the moment a leader
     is elected without waiting on replication timing.
  4. ACKED READ CONSISTENCY -- once the current leader has applied up to an
     acked write's index, reading that key from the leader returns that
     write's value (or a newer acked one).

Deliberately NOT checked: stale reads from followers. ARCHITECTURE.md §5
documents that reads are served locally with no read-index or leader lease,
so a lagging follower returning an old value is designed behaviour. Those are
counted and reported separately, never as violations.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from raft.node import Role

from chaos.events import EventKind


class ViolationKind:
    ELECTION_SAFETY = "election_safety"
    STATE_MACHINE_SAFETY = "state_machine_safety"
    LEADER_COMPLETENESS = "leader_completeness"
    ACKED_READ = "acked_read_consistency"


@dataclass
class PendingWrite:
    index: int
    term: int
    command: dict
    proposed_by: str
    proposed_tick: int
    acked_tick: int | None = None
    lost: bool = False

    @property
    def key(self) -> str | None:
        return self.command.get("key")


@dataclass
class CheckReport:
    violations: list[dict] = field(default_factory=list)
    acked_writes: int = 0
    proposed_writes: int = 0
    lost_proposals: int = 0
    stale_follower_reads: int = 0

    @property
    def passed(self) -> bool:
        return not self.violations

    def to_json(self) -> dict:
        return {
            "passed": self.passed,
            "violations": self.violations,
            "acked_writes": self.acked_writes,
            "proposed_writes": self.proposed_writes,
            "lost_proposals": self.lost_proposals,
            "stale_follower_reads": self.stale_follower_reads,
        }


class CorrectnessChecker:
    def __init__(self, cluster) -> None:
        self.cluster = cluster
        self.report = CheckReport()

        self.pending: list[PendingWrite] = []
        # index -> (term, command) for entries that reached ack
        self.acked_by_index: dict[int, tuple[int, dict]] = {}
        # key -> (index, value) of the newest acked write for that key
        self.latest_acked_for_key: dict[str, tuple[int, str]] = {}
        # canonical applied history, index -> (term, command)
        self._canonical: dict[int, tuple[int, dict]] = {}
        # term -> leaders seen in that term
        self._leaders_by_term: dict[int, set[str]] = {}
        self._checked_leader_terms: set[tuple[str, int]] = set()

    # ------------------------------------------------------------ registration

    def track(self, leader_id: str, entry) -> PendingWrite:
        write = PendingWrite(
            index=entry.index, term=entry.term, command=entry.command,
            proposed_by=leader_id, proposed_tick=self.cluster.current_tick,
        )
        self.pending.append(write)
        self.report.proposed_writes += 1
        return write

    # ------------------------------------------------------------- the observer

    def __call__(self, cluster) -> None:
        self._resolve_pending()
        self._check_election_safety()
        self._check_state_machine_safety()
        self._check_leader_completeness()
        self._check_acked_reads()

    def _violation(self, kind: str, message: str, **detail) -> None:
        record = {
            "kind": kind,
            "tick": self.cluster.current_tick,
            "message": message,
            **detail,
        }
        self.report.violations.append(record)
        self.cluster.log.record(self.cluster.current_tick, EventKind.VIOLATION, **record)

    # ----------------------------------------------------------------- checks

    def _resolve_pending(self) -> None:
        """A write is acknowledged when the leader that accepted it has
        advanced commit_index past it while still holding that entry at the
        same term -- the moment a real server would answer the client."""
        for write in self.pending:
            if write.acked_tick is not None or write.lost:
                continue

            # Any alive node whose commit_index covers this index, and whose
            # log still holds this exact entry, is a valid witness that it
            # committed -- commit is a cluster-wide property, not a property
            # of the original proposer. Watching only the proposer means an
            # entry that committed long ago is not noticed until that node
            # happens to rejoin, which back-dates nothing but mis-orders
            # acknowledgements against each other.
            node = self._ack_witness(write)
            if node is None:
                node = self.cluster.nodes.get(write.proposed_by)
                if node is None or not self.cluster.alive.get(write.proposed_by, False):
                    continue
            entry = node.log.get(write.index)
            if entry is None or entry.term != write.term:
                # Overwritten by a later leader before committing. Correct
                # Raft behaviour for an *uncommitted* entry -- not a loss.
                write.lost = True
                self.report.lost_proposals += 1
                self.cluster.log.record(
                    self.cluster.current_tick, EventKind.WRITE_LOST,
                    index=write.index, term=write.term, command=write.command,
                    note="uncommitted entry overwritten before ack (correct behaviour)",
                )
                continue
            if node.commit_index >= write.index:
                write.acked_tick = self.cluster.current_tick
                self.report.acked_writes += 1
                self.acked_by_index[write.index] = (write.term, write.command)
                if write.key is not None and write.command.get("op") == "set":
                    # Newest by LOG INDEX, never by detection order. An entry
                    # whose proposer was partitioned can be noticed after a
                    # higher index has already been acknowledged; taking the
                    # later detection would regress the expected value to one
                    # a subsequent write legitimately superseded, and report a
                    # violation against correct behaviour.
                    previous = self.latest_acked_for_key.get(write.key)
                    if previous is None or write.index > previous[0]:
                        self.latest_acked_for_key[write.key] = (write.index, write.command["value"])
                self.cluster.log.record(
                    self.cluster.current_tick, EventKind.WRITE_ACKED,
                    index=write.index, term=write.term, command=write.command,
                    leader=write.proposed_by,
                )

    def _ack_witness(self, write: PendingWrite):
        """An alive node proving this entry committed: it holds the entry at
        the same index and term, and its commit_index covers it."""
        for nid, node in self.cluster.nodes.items():
            if not self.cluster.alive.get(nid, False):
                continue
            if node.commit_index < write.index:
                continue
            entry = node.log.get(write.index)
            if entry is not None and entry.term == write.term and entry.command == write.command:
                return node
        return None

    def _check_election_safety(self) -> None:
        for nid, node in self.cluster.nodes.items():
            if not self.cluster.alive[nid] or node.role != Role.LEADER:
                continue
            seen = self._leaders_by_term.setdefault(node.current_term, set())
            seen.add(nid)
            if len(seen) > 1:
                self._violation(
                    ViolationKind.ELECTION_SAFETY,
                    f"two leaders in term {node.current_term}: {sorted(seen)}",
                    term=node.current_term, leaders=sorted(seen),
                )

    def _check_state_machine_safety(self) -> None:
        for nid in self.cluster.node_ids:
            for index, entry in self.cluster.applied_log[nid].items():
                known = self._canonical.get(index)
                if known is None:
                    self._canonical[index] = (entry.term, entry.command)
                    continue
                known_term, known_command = known
                if known_command != entry.command:
                    self._violation(
                        ViolationKind.STATE_MACHINE_SAFETY,
                        f"log index {index} applied as two different commands",
                        index=index, node=nid,
                        first_seen={"term": known_term, "command": known_command},
                        conflicting={"term": entry.term, "command": entry.command},
                    )
                    # Adopt the newer value so the same divergence is not
                    # re-reported on every subsequent tick.
                    self._canonical[index] = (entry.term, entry.command)

    def _check_leader_completeness(self) -> None:
        for nid, node in self.cluster.nodes.items():
            if not self.cluster.alive[nid] or node.role != Role.LEADER:
                continue
            marker = (nid, node.current_term)
            if marker in self._checked_leader_terms:
                continue
            self._checked_leader_terms.add(marker)

            for index, (term, command) in sorted(self.acked_by_index.items()):
                entry = node.log.get(index)
                if entry is None:
                    self._violation(
                        ViolationKind.LEADER_COMPLETENESS,
                        f"leader {nid} (term {node.current_term}) is missing acked entry at index {index}",
                        index=index, leader=nid, term=node.current_term, expected_command=command,
                    )
                elif entry.command != command:
                    self._violation(
                        ViolationKind.LEADER_COMPLETENESS,
                        f"leader {nid} has a different command at acked index {index}",
                        index=index, leader=nid, term=node.current_term,
                        expected_command=command, found_command=entry.command,
                    )

    def _check_acked_reads(self) -> None:
        leaders = self.cluster.current_leaders()
        if len(leaders) != 1:
            return  # mid-transition; election safety covers the >1 case
        leader_id = leaders[0]
        node = self.cluster.nodes[leader_id]

        for key, (index, value) in self.latest_acked_for_key.items():
            if node.last_applied < index:
                continue  # hasn't caught up to this write yet; not a violation
            observed = self.cluster.read(leader_id, key)
            if observed != value:
                self._violation(
                    ViolationKind.ACKED_READ,
                    f"leader {leader_id} read key {key!r} as {observed!r}, "
                    f"but {value!r} was acknowledged at index {index}",
                    key=key, expected=value, observed=observed,
                    index=index, leader=leader_id,
                )

    def count_stale_follower_reads(self, key: str) -> None:
        """Observational only -- documented behaviour, never a violation."""
        expected = self.latest_acked_for_key.get(key)
        if expected is None:
            return
        _, value = expected
        for nid in self.cluster.node_ids:
            if not self.cluster.alive[nid] or nid in self.cluster.current_leaders():
                continue
            if self.cluster.read(nid, key) != value:
                self.report.stale_follower_reads += 1
