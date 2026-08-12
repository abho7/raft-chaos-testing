"""
A chaos transport for RaftNode.

The engine ships two transports already: `raft/simulator.py` (in-memory,
delivery is immediate and recursive inside one tick) and
`kvstore/server.py` (real asyncio TCP). This is a third, and it exists for
one reason the other two cannot cover: **a message queue**.

`SimulatedCluster._deliver` calls the recipient's handler synchronously and
recurses, so a message has no in-flight state and "delay this message by five
ticks" has nowhere to live. `node.py`'s own docstring anticipates this -- it
says a harness may "deliver it, delay it, or drop it" -- so the queue goes
here, in a transport, and RaftNode is used completely unmodified.

Delivery model, stated plainly:
  * A message sent on tick T arrives no earlier than T + base_latency
    (default 1), so nothing is instantaneous and responses never resolve
    inside the tick that produced them.
  * Per-link drop is rolled at *send* time -- that models packet loss.
  * Partitions and node liveness are checked at *delivery* time -- that
    models a split killing messages already in flight.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from kvstore.state_machine import KVStateMachine
from raft.messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)
from raft.node import Actions, RaftNode, Role

from chaos.events import EventKind, EventLog
from chaos.faults import Fault


@dataclass(order=True)
class _Queued:
    deliver_at: int
    seq: int
    sender: str = field(compare=False)
    recipient: str = field(compare=False)
    message: object = field(compare=False)


class ChaosCluster:
    def __init__(
        self,
        node_ids: list[str],
        *,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        base_latency: int = 1,
        seed: int = 0,
    ):
        self.node_ids = list(node_ids)
        self.nodes: dict[str, RaftNode] = {
            nid: RaftNode(
                nid,
                [p for p in node_ids if p != nid],
                election_timeout_ticks=election_timeout_ticks,
                heartbeat_interval_ticks=heartbeat_interval_ticks,
                random_seed=f"{seed}-{nid}",
            )
            for nid in node_ids
        }
        self.alive: dict[str, bool] = {nid: True for nid in node_ids}

        # Keyed by log index rather than appended, for the reason the engine
        # documents in SimulatedCluster: a restarted node's last_applied
        # resets to 0, so it legitimately re-reports its whole committed
        # prefix. Overwriting by index makes that a no-op instead of a
        # phantom duplicate.
        self.applied_log: dict[str, dict[int, LogEntry]] = {nid: {} for nid in node_ids}
        self.state_machines: dict[str, KVStateMachine] = {nid: KVStateMachine() for nid in node_ids}

        self.faults: list[Fault] = []
        self.log = EventLog()
        self.current_tick = 0

        self._queue: list[_Queued] = []
        self._seq = 0
        self._base_latency = base_latency
        self._rng = random.Random(seed)
        self._election_timeout_range = election_timeout_ticks
        self._heartbeat_interval = heartbeat_interval_ticks

        # Snapshots so tick-to-tick transitions can be turned into events.
        self._last_roles: dict[str, Role] = {nid: n.role for nid, n in self.nodes.items()}
        self._last_terms: dict[str, int] = {nid: n.current_term for nid, n in self.nodes.items()}
        self._last_commit: dict[str, int] = {nid: 0 for nid in node_ids}

        self.observers: list = []

    # ------------------------------------------------------------ fault API

    def inject(self, fault: Fault) -> Fault:
        fault.started_tick = self.current_tick
        self.faults.append(fault)
        self.log.record(self.current_tick, EventKind.FAULT_INJECTED, **fault.describe())
        return fault

    def heal(self, fault: Fault) -> None:
        if fault in self.faults:
            fault.healed_tick = self.current_tick
            self.faults.remove(fault)
            self.log.record(self.current_tick, EventKind.FAULT_HEALED, **fault.describe())

    def heal_all(self) -> None:
        for fault in list(self.faults):
            self.heal(fault)

    def kill_node(self, node_id: str) -> None:
        self.alive[node_id] = False
        self.log.record(self.current_tick, EventKind.FAULT_INJECTED, kind="NodeCrash", node=node_id)

    def restart_node(self, node_id: str) -> None:
        """Crash + restart: only term, vote and log survive, matching the
        Raft paper and the engine's own restart semantics."""
        old = self.nodes[node_id]
        fresh = RaftNode(
            node_id,
            old.peer_ids,
            election_timeout_ticks=self._election_timeout_range,
            heartbeat_interval_ticks=self._heartbeat_interval,
            random_seed=f"restart-{self.current_tick}-{node_id}",
        )
        fresh.current_term = old.current_term
        fresh.voted_for = old.voted_for
        fresh.log = old.log
        self.nodes[node_id] = fresh
        self.alive[node_id] = True
        self._last_roles[node_id] = fresh.role
        self.log.record(self.current_tick, EventKind.FAULT_HEALED, kind="NodeRestart", node=node_id)

    # ------------------------------------------------------------ transport

    def _link_delay(self, sender: str, recipient: str) -> int:
        return self._base_latency + sum(f.extra_delay(sender, recipient) for f in self.faults)

    def _drop_roll(self, sender: str, recipient: str) -> bool:
        for fault in self.faults:
            p = fault.drop_probability(sender, recipient)
            if p > 0 and self._rng.random() < p:
                return True
        return False

    def _reachable(self, sender: str, recipient: str) -> bool:
        if not self.alive.get(sender, False) or not self.alive.get(recipient, False):
            return False
        return not any(f.blocks(sender, recipient) for f in self.faults)

    def _enqueue(self, sender: str, recipient: str, message) -> None:
        self.log.messages_sent += 1
        if self._drop_roll(sender, recipient):
            self.log.messages_dropped += 1
            self.log.record(
                self.current_tick, EventKind.MESSAGE_DROPPED,
                sender=sender, recipient=recipient, reason="link_drop",
                message=type(message).__name__,
            )
            return

        delay = self._link_delay(sender, recipient)
        if delay > self._base_latency:
            self.log.messages_delayed += 1
        self._seq += 1
        self._queue.append(
            _Queued(deliver_at=self.current_tick + delay, seq=self._seq,
                    sender=sender, recipient=recipient, message=message)
        )

    def _dispatch(self, actions: Actions, origin: str) -> None:
        for entry in actions.committed_entries:
            self.applied_log[origin][entry.index] = entry
            self.state_machines[origin].apply(entry)
        for recipient, message in actions.messages:
            self._enqueue(origin, recipient, message)

    def _deliver_due(self) -> None:
        due = [q for q in self._queue if q.deliver_at <= self.current_tick]
        if not due:
            return
        self._queue = [q for q in self._queue if q.deliver_at > self.current_tick]
        for item in sorted(due):
            if not self._reachable(item.sender, item.recipient):
                self.log.messages_dropped += 1
                self.log.record(
                    self.current_tick, EventKind.MESSAGE_DROPPED,
                    sender=item.sender, recipient=item.recipient,
                    reason="partitioned_or_dead", message=type(item.message).__name__,
                )
                continue

            self.log.messages_delivered += 1
            target = self.nodes[item.recipient]
            msg = item.message
            if isinstance(msg, RequestVoteRequest):
                actions = target.handle_request_vote(item.sender, msg)
            elif isinstance(msg, RequestVoteResponse):
                actions = target.handle_request_vote_response(item.sender, msg)
            elif isinstance(msg, AppendEntriesRequest):
                actions = target.handle_append_entries(item.sender, msg)
            elif isinstance(msg, AppendEntriesResponse):
                actions = target.handle_append_entries_response(item.sender, msg)
            else:
                raise TypeError(f"Unknown message type: {type(msg)!r}")
            self._dispatch(actions, item.recipient)

    # -------------------------------------------------------------- stepping

    def tick(self) -> None:
        self.current_tick += 1
        self._deliver_due()
        for nid in self.node_ids:
            if not self.alive[nid]:
                continue
            self._dispatch(self.nodes[nid].tick(), nid)
        self._emit_transitions()
        for observer in self.observers:
            observer(self)

    def run_ticks(self, n: int) -> None:
        for _ in range(n):
            self.tick()

    def _emit_transitions(self) -> None:
        for nid, node in self.nodes.items():
            if not self.alive[nid]:
                continue
            if node.current_term != self._last_terms[nid]:
                self.log.record(self.current_tick, EventKind.TERM_CHANGE,
                                node=nid, term=node.current_term)
                self._last_terms[nid] = node.current_term
            if node.role != self._last_roles[nid]:
                self.log.record(self.current_tick, EventKind.ROLE_CHANGE, node=nid,
                                previous=self._last_roles[nid].value, role=node.role.value,
                                term=node.current_term)
                if node.role == Role.LEADER:
                    self.log.record(self.current_tick, EventKind.LEADER_ELECTED,
                                    node=nid, term=node.current_term)
                self._last_roles[nid] = node.role
            if node.commit_index != self._last_commit[nid]:
                self.log.record(self.current_tick, EventKind.COMMIT_ADVANCED,
                                node=nid, commit_index=node.commit_index)
                self._last_commit[nid] = node.commit_index

    # ------------------------------------------------------------- client API

    def leader(self) -> str | None:
        leaders = self.current_leaders()
        return leaders[0] if len(leaders) == 1 else (leaders[0] if leaders else None)

    def current_leaders(self) -> list[str]:
        return [nid for nid, n in self.nodes.items() if self.alive[nid] and n.role == Role.LEADER]

    def propose_to(self, node_id: str, command: dict) -> tuple[str, LogEntry] | None:
        """Propose to one named node, which accepts only if it believes it is
        leader.

        Needed because `propose()` scans node_ids in order and will happily
        hand a write to an isolated stale leader that still thinks it is in
        charge. That is realistic client behaviour, but it makes a scenario's
        outcome depend on node naming -- so any scenario asserting "the
        majority side keeps committing" must say which side it means.
        """
        node = self.nodes[node_id]
        if not self.alive[node_id] or node.role != Role.LEADER:
            return None
        entry, actions = node.propose(command)
        if entry is None:
            return None
        self._dispatch(actions, node_id)
        self.log.record(self.current_tick, EventKind.WRITE_PROPOSED,
                        leader=node_id, index=entry.index, term=entry.term, command=command)
        return node_id, entry

    def leader_within(self, candidates: set[str]) -> str | None:
        """The leader inside a given subset -- e.g. the majority side of a
        partition, where several nodes may believe they are leader at once."""
        found = [n for n in candidates if self.alive[n] and self.nodes[n].role == Role.LEADER]
        if not found:
            return None
        # Highest term wins: that is the one whose writes can actually commit.
        return max(found, key=lambda n: self.nodes[n].current_term)

    def propose(self, command: dict) -> tuple[str, LogEntry] | None:
        """Propose to whichever alive node currently believes it is leader.

        Returns (leader_id, entry) -- note this is NOT an acknowledgement.
        The entry is merely appended to that leader's log; it counts as
        acked only once that leader's commit_index covers it, which is what
        `PendingWrite` in checker.py tracks.
        """
        for nid in self.node_ids:
            node = self.nodes[nid]
            if self.alive[nid] and node.role == Role.LEADER:
                entry, actions = node.propose(command)
                if entry is None:
                    return None
                self._dispatch(actions, nid)
                self.log.record(self.current_tick, EventKind.WRITE_PROPOSED,
                                leader=nid, index=entry.index, term=entry.term, command=command)
                return nid, entry
        return None

    def run_until_leader(self, max_ticks: int = 300) -> str | None:
        for _ in range(max_ticks):
            self.tick()
            leaders = self.current_leaders()
            if len(leaders) == 1:
                return leaders[0]
        return None

    def applied_commands(self, node_id: str) -> list[dict]:
        entries = self.applied_log[node_id]
        return [entries[i].command for i in sorted(entries)]

    def read(self, node_id: str, key: str) -> str | None:
        """Read straight from a node's state machine, exactly as the engine's
        kvstore server does. Reads are documented as non-linearizable, so a
        follower read may legitimately be stale -- only leader reads are held
        to the consistency check."""
        return self.state_machines[node_id].get(key)
