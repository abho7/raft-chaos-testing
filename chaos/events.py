"""
Structured events emitted while a scenario runs.

Everything the report shows -- timelines, message counts, the exact tick a
violation appeared -- is reconstructed from this stream. Nothing is computed
only at the end, because "the cluster was consistent when we finally looked"
is a much weaker claim than "the cluster was consistent at every tick."
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum


class EventKind(str, Enum):
    # cluster lifecycle
    ROLE_CHANGE = "role_change"
    LEADER_ELECTED = "leader_elected"
    TERM_CHANGE = "term_change"
    COMMIT_ADVANCED = "commit_advanced"

    # client activity
    WRITE_PROPOSED = "write_proposed"
    WRITE_ACKED = "write_acked"
    WRITE_LOST = "write_lost"

    # fault injection
    FAULT_INJECTED = "fault_injected"
    FAULT_HEALED = "fault_healed"

    # transport
    MESSAGE_DROPPED = "message_dropped"
    MESSAGE_DELAYED = "message_delayed"

    # correctness
    VIOLATION = "violation"


@dataclass
class Event:
    tick: int
    kind: EventKind
    detail: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        out = asdict(self)
        out["kind"] = self.kind.value
        return out


class EventLog:
    """Append-only event stream plus a few counters the report needs."""

    def __init__(self) -> None:
        self.events: list[Event] = []
        self.messages_sent = 0
        self.messages_delivered = 0
        self.messages_dropped = 0
        self.messages_delayed = 0

    def record(self, tick: int, kind: EventKind, /, **detail) -> Event:
        """`tick` and `kind` are positional-only so callers may pass their own
        "kind" inside detail -- Fault.describe() legitimately does."""
        event = Event(tick=tick, kind=kind, detail=detail)
        self.events.append(event)
        return event

    def of_kind(self, *kinds: EventKind) -> list[Event]:
        wanted = set(kinds)
        return [e for e in self.events if e.kind in wanted]

    @property
    def violations(self) -> list[Event]:
        return self.of_kind(EventKind.VIOLATION)

    def to_json(self) -> list[dict]:
        return [e.to_json() for e in self.events]

    def counters(self) -> dict:
        return {
            "messages_sent": self.messages_sent,
            "messages_delivered": self.messages_delivered,
            "messages_dropped": self.messages_dropped,
            "messages_delayed": self.messages_delayed,
        }
