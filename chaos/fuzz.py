"""
Randomized fault sweep.

The six named scenarios are hand-built and reproducible, which makes them good
report material but poor bug hunters -- they only probe the interleavings
somebody thought of. This sweep generates random fault schedules across many
seeds instead, so "no violations found" is a claim backed by search rather
than by six lucky orderings.

Any seed that violates is printed and can be replayed exactly, since every
source of randomness here is seeded.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from chaos.checker import CorrectnessChecker
from chaos.cluster import ChaosCluster
from chaos.faults import LinkFault, Partition


@dataclass
class FuzzOutcome:
    seed: int
    violations: list[dict]
    acked_writes: int
    ticks: int
    actions: list[str]

    @property
    def passed(self) -> bool:
        return not self.violations


def run_one(seed: int, *, nodes: int = 5, rounds: int = 12) -> FuzzOutcome:
    node_ids = [f"n{i + 1}" for i in range(nodes)]
    rng = random.Random(seed * 7919 + 13)

    cluster = ChaosCluster(node_ids, seed=seed)
    checker = CorrectnessChecker(cluster)
    cluster.observers.append(checker)
    cluster.run_until_leader(300)

    actions: list[str] = []
    active: list = []

    for _ in range(rounds):
        choice = rng.random()

        if choice < 0.30:
            result = cluster.propose({"op": "set", "key": rng.choice("abcde"), "value": str(rng.randint(0, 99))})
            if result is not None:
                checker.track(*result)
                actions.append(f"write idx={result[1].index}")

        elif choice < 0.45:
            victim = rng.choice([n for n in node_ids if cluster.alive[n]])
            # Never drop below quorum by crashing -- that only tests stalling,
            # which the named quorum-loss scenario already covers on purpose.
            if sum(cluster.alive.values()) - 1 > nodes // 2:
                cluster.kill_node(victim)
                actions.append(f"kill {victim}")

        elif choice < 0.55:
            dead = [n for n in node_ids if not cluster.alive[n]]
            if dead:
                node = rng.choice(dead)
                cluster.restart_node(node)
                actions.append(f"restart {node}")

        elif choice < 0.75:
            size = rng.randint(1, nodes // 2)
            group = set(rng.sample(node_ids, size))
            fault = cluster.inject(Partition(label=f"split {sorted(group)}",
                                             groups=[group, set(node_ids) - group]))
            active.append(fault)
            actions.append(f"partition {sorted(group)}")

        elif choice < 0.90:
            a, b = rng.sample(node_ids, 2)
            fault = cluster.inject(LinkFault(
                label=f"{a}~{b}", sender=a, recipient=b,
                drop_prob=rng.choice([0.0, 0.3, 0.7]),
                delay_ticks=rng.choice([0, 3, 9]),
                bidirectional=True,
            ))
            active.append(fault)
            actions.append(f"link {a}~{b}")

        elif active:
            fault = active.pop(rng.randrange(len(active)))
            cluster.heal(fault)
            actions.append("heal")

        cluster.run_ticks(rng.randint(10, 45))

    # Settle: heal everything and let the cluster converge, which is where a
    # latent divergence would finally surface.
    cluster.heal_all()
    for node in node_ids:
        if not cluster.alive[node]:
            cluster.restart_node(node)
    cluster.run_ticks(250)

    return FuzzOutcome(
        seed=seed,
        violations=checker.report.violations,
        acked_writes=checker.report.acked_writes,
        ticks=cluster.current_tick,
        actions=actions,
    )


def sweep(seed_count: int = 200, *, verbose: bool = True) -> list[FuzzOutcome]:
    failures: list[FuzzOutcome] = []
    total_acked = 0

    for seed in range(seed_count):
        outcome = run_one(seed)
        total_acked += outcome.acked_writes
        if not outcome.passed:
            failures.append(outcome)
            if verbose:
                print(f"  VIOLATION seed={seed}: {len(outcome.violations)} found")
                for violation in outcome.violations[:3]:
                    print(f"      {violation['kind']} @tick {violation['tick']}: {violation['message']}")
                print(f"      replay: {' -> '.join(outcome.actions)}")

    if verbose:
        print(f"\n{seed_count} seeds, {total_acked} acknowledged writes, {len(failures)} seeds with violations")
    return failures


if __name__ == "__main__":
    sweep()
