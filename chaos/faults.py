"""
Composable fault definitions.

Each fault answers the same two questions about a directed link, and the
cluster combines every active fault to decide what happens to a message.
Composability falls out of that: a partition and a slow link and a crashed
node are three independent answers, not three mutually exclusive modes.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Fault:
    """Base fault. `label` is what shows up on the report's timeline."""

    label: str = "fault"
    started_tick: int | None = None
    healed_tick: int | None = None

    def blocks(self, sender: str, recipient: str) -> bool:
        return False

    def extra_delay(self, sender: str, recipient: str) -> int:
        return 0

    def drop_probability(self, sender: str, recipient: str) -> float:
        return 0.0

    def describe(self) -> dict:
        return {"kind": type(self).__name__, "label": self.label}


@dataclass
class Partition(Fault):
    """Splits the cluster into groups that cannot talk across group lines.

    Unlike the engine's own `SimulatedCluster.partition`, this supports more
    than two groups and more than one simultaneous partition, because several
    Partition instances can be active at once and each is consulted
    independently.
    """

    groups: list[set[str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for group in self.groups:
            overlap = seen & group
            if overlap:
                raise ValueError(f"partition groups must be disjoint; {sorted(overlap)} repeated")
            seen |= group

    def _group_of(self, node: str) -> int | None:
        for i, group in enumerate(self.groups):
            if node in group:
                return i
        return None

    def blocks(self, sender: str, recipient: str) -> bool:
        a, b = self._group_of(sender), self._group_of(recipient)
        # A node named in no group is unconstrained by this partition, which
        # is what lets several partial partitions compose.
        if a is None or b is None:
            return False
        return a != b

    def describe(self) -> dict:
        return {
            "kind": "Partition",
            "label": self.label,
            "groups": [sorted(g) for g in self.groups],
        }


@dataclass
class LinkFault(Fault):
    """Degrades one specific directed link (or both directions).

    This is the capability the engine's global `drop_rate` cannot express:
    it targets a named sender/recipient pair rather than every message
    uniformly, and it can delay rather than only drop.
    """

    sender: str = ""
    recipient: str = ""
    drop_prob: float = 0.0
    delay_ticks: int = 0
    bidirectional: bool = False

    def _applies(self, sender: str, recipient: str) -> bool:
        if sender == self.sender and recipient == self.recipient:
            return True
        return self.bidirectional and sender == self.recipient and recipient == self.sender

    def extra_delay(self, sender: str, recipient: str) -> int:
        return self.delay_ticks if self._applies(sender, recipient) else 0

    def drop_probability(self, sender: str, recipient: str) -> float:
        return self.drop_prob if self._applies(sender, recipient) else 0.0

    def describe(self) -> dict:
        return {
            "kind": "LinkFault",
            "label": self.label,
            "sender": self.sender,
            "recipient": self.recipient,
            "drop_prob": self.drop_prob,
            "delay_ticks": self.delay_ticks,
            "bidirectional": self.bidirectional,
        }
