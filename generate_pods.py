#!/usr/bin/env python3
"""Generate cEDH tournament pods with minimized duplicate matchups.

Input format (tournament_input.txt):
Games (total): <int>
Players (enter 1 player per line):
<player 1>
<player 2>
...

Output format (tournament_output.gsheet):
- Pods by game
- Bye list (if any)
- Duplicate matchup summary per player
"""

from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import math
import os
import random
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GameResult:
    pods: list[list[str]]
    byes: list[str]


@dataclass
class SearchConfig:
    round_attempts: int
    schedule_attempts: int
    candidate_cap: int
    plateau_limit: int


def parse_input(path: Path) -> tuple[int, list[str]]:
    text = path.read_text(encoding="utf-8")
    text = text.lstrip("\ufeff")
    lines = [line.rstrip() for line in text.splitlines()]

    games: int | None = None
    players: list[str] = []

    for line in lines:
        match = re.match(r"^\s*Games\s*\(total\)\s*:\s*(\d+)\s*$", line, re.IGNORECASE)
        if match:
            games = int(match.group(1))
            break

    in_players = False
    for line in lines:
        if re.match(r"^\s*Players\b", line, re.IGNORECASE):
            in_players = True
            continue
        if not in_players:
            continue
        name = line.strip()
        if name:
            players.append(name)

    if games is None:
        for line in lines:
            if line.strip().isdigit():
                games = int(line.strip())
                break

    if games is None or games <= 0:
        raise ValueError("Could not parse a positive game count from input.")
    if len(players) < 3:
        raise ValueError("Need at least 3 players to generate pods.")

    deduped: list[str] = []
    seen: set[str] = set()
    for player in players:
        if player in seen:
            raise ValueError(f"Duplicate player name found: {player}")
        seen.add(player)
        deduped.append(player)

    return games, deduped


def pair_penalty(existing: int) -> int:
    """Cost for pairing players that have already met `existing` times."""
    if existing <= 0:
        return 0
    if existing == 1:
        return 3
    return 100 + (existing - 2) * 50


def combos2(n: int) -> int:
    return n * (n - 1) // 2


def pod_sizes_for_count(player_count: int) -> tuple[list[int], int]:
    base = player_count // 4
    rem = player_count % 4
    sizes = [4] * base
    byes = 0

    if rem == 3:
        sizes.append(3)
    elif rem in (1, 2):
        byes = rem

    return sizes, byes


def choose_byes(players: list[str], bye_count: int, bye_totals: dict[str, int], rng: random.Random) -> list[str]:
    if bye_count <= 0:
        return []
    shuffled = players[:]
    rng.shuffle(shuffled)
    ordered = sorted(shuffled, key=lambda p: bye_totals[p])
    return ordered[:bye_count]


def pod_cost(
    pod: tuple[str, ...], pair_counts: dict[tuple[str, str], int], max_pair_meet: int
) -> int:
    cost = 0
    for a, b in itertools.combinations(sorted(pod), 2):
        existing = pair_counts[(a, b)]
        cost += pair_penalty(existing)
        if existing >= max_pair_meet:
            # Strongly discourage over-cap pairings, but still allow as fallback.
            cost += 1_000
    return cost


def sample_candidate_pods(
    remaining: list[str], size: int, rng: random.Random, cap: int = 400
) -> list[tuple[str, ...]]:
    combos_total = 0
    n = len(remaining)
    if size == 4 and n >= 4:
        combos_total = n * (n - 1) * (n - 2) * (n - 3) // 24
    elif size == 3 and n >= 3:
        combos_total = n * (n - 1) * (n - 2) // 6

    if combos_total <= cap:
        return list(itertools.combinations(remaining, size))

    seen: set[tuple[str, ...]] = set()
    sampled: list[tuple[str, ...]] = []
    attempts = 0
    max_attempts = cap * 20
    while len(sampled) < cap and attempts < max_attempts:
        attempts += 1
        cand = tuple(sorted(rng.sample(remaining, size)))
        if cand in seen:
            continue
        seen.add(cand)
        sampled.append(cand)
    return sampled


def tune_search(player_count: int, games: int) -> SearchConfig:
    workload = player_count * games
    if workload >= 700:
        return SearchConfig(round_attempts=30, schedule_attempts=6, candidate_cap=80, plateau_limit=3)
    if workload >= 350:
        return SearchConfig(round_attempts=50, schedule_attempts=8, candidate_cap=100, plateau_limit=4)
    if workload >= 180:
        return SearchConfig(round_attempts=90, schedule_attempts=12, candidate_cap=140, plateau_limit=5)
    return SearchConfig(round_attempts=180, schedule_attempts=16, candidate_cap=220, plateau_limit=6)


def total_pair_events_per_game(player_count: int) -> int:
    sizes, _ = pod_sizes_for_count(player_count)
    return sum(combos2(size) for size in sizes)


def minimum_feasible_cap(player_count: int, games: int) -> int:
    unique_pairs = combos2(player_count)
    if unique_pairs <= 0:
        return 0
    total_events = total_pair_events_per_game(player_count) * games
    return math.ceil(total_events / unique_pairs)


def build_round(
    available_players: list[str],
    sizes: list[int],
    pair_counts: dict[tuple[str, str], int],
    rng: random.Random,
    max_pair_meet: int,
    attempts: int,
    candidate_cap: int,
) -> list[list[str]]:
    best_pods: list[list[str]] | None = None
    best_score: int | None = None

    for _ in range(attempts):
        remaining = available_players[:]
        rng.shuffle(remaining)
        pods: list[list[str]] = []
        total_score = 0
        valid = True

        for size in sizes:
            candidates = sample_candidate_pods(remaining, size, rng, cap=candidate_cap)
            if not candidates:
                valid = False
                break
            cost_cache: dict[tuple[str, ...], int] = {}
            valid_candidates = []
            for cand in candidates:
                exceeds_cap = False
                for a, b in itertools.combinations(sorted(cand), 2):
                    if pair_counts[(a, b)] >= max_pair_meet:
                        exceeds_cap = True
                        break
                if not exceeds_cap:
                    valid_candidates.append(cand)
                cost_cache[cand] = pod_cost(cand, pair_counts, max_pair_meet)

            working = valid_candidates if valid_candidates else candidates
            rng.shuffle(working)
            chosen = min(working, key=lambda c: cost_cache[c])
            total_score += cost_cache[chosen]
            pods.append(list(chosen))
            chosen_set = set(chosen)
            remaining = [p for p in remaining if p not in chosen_set]

        if not valid:
            continue
        if remaining:
            valid = False
        if not valid:
            continue

        if best_score is None or total_score < best_score:
            best_score = total_score
            best_pods = pods

    if best_pods is None:
        raise RuntimeError("Failed to build a valid round.")

    return best_pods


def schedule_score(pair_counts: dict[tuple[str, str], int], max_pair_meet: int) -> tuple[int, int]:
    over_cap = 0
    duplicates = 0
    for count in pair_counts.values():
        if count > 1:
            duplicates += count - 1
        if count > max_pair_meet:
            over_cap += count - max_pair_meet
    return over_cap, duplicates


def run_schedule_attempt(
    games: int,
    players: list[str],
    seed: int,
    max_pair_meet: int,
    config: SearchConfig,
) -> tuple[list[GameResult], dict[tuple[str, str], int], tuple[int, int]]:
    rng = random.Random(seed)
    pair_counts: dict[tuple[str, str], int] = defaultdict(int)
    bye_totals: dict[str, int] = defaultdict(int)
    results: list[GameResult] = []

    sizes, bye_count = pod_sizes_for_count(len(players))
    bye_set: set[str]
    for _round in range(games):
        byes = choose_byes(players, bye_count, bye_totals, rng)
        for p in byes:
            bye_totals[p] += 1
        bye_set = set(byes)
        active = [p for p in players if p not in bye_set]

        pods = build_round(
            active,
            sizes,
            pair_counts,
            rng,
            max_pair_meet=max_pair_meet,
            attempts=config.round_attempts,
            candidate_cap=config.candidate_cap,
        )

        for pod in pods:
            for a, b in itertools.combinations(sorted(pod), 2):
                pair_counts[(a, b)] += 1

        results.append(GameResult(pods=pods, byes=sorted(byes)))

    return results, pair_counts, schedule_score(pair_counts, max_pair_meet)


def generate_schedule(
    games: int,
    players: list[str],
    seed: int = 7,
    max_pair_meet: int = 2,
    parallel_workers: int = 1,
    config: SearchConfig | None = None,
) -> tuple[list[GameResult], dict[tuple[str, str], int]]:
    if config is None:
        config = tune_search(len(players), games)

    best_results: list[GameResult] | None = None
    best_pair_counts: dict[tuple[str, str], int] | None = None
    best_score: tuple[int, int] | None = None
    no_improve = 0
    attempt_seeds = [seed + i for i in range(config.schedule_attempts)]
    workers = max(1, parallel_workers)

    if workers == 1:
        for try_seed in attempt_seeds:
            results, pair_counts, current_score = run_schedule_attempt(
                games, players, try_seed, max_pair_meet, config
            )
            if best_score is None or current_score < best_score:
                best_results = results
                best_pair_counts = pair_counts
                best_score = current_score
                no_improve = 0
                if best_score[0] == 0:
                    break
            else:
                no_improve += 1
                if no_improve >= config.plateau_limit:
                    break
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(run_schedule_attempt, games, players, try_seed, max_pair_meet, config)
                for try_seed in attempt_seeds
            ]
            for fut in concurrent.futures.as_completed(futures):
                results, pair_counts, current_score = fut.result()
                if best_score is None or current_score < best_score:
                    best_results = results
                    best_pair_counts = pair_counts
                    best_score = current_score
                    if best_score[0] == 0:
                        for pending in futures:
                            pending.cancel()
                        break

    if best_results is None or best_pair_counts is None:
        raise RuntimeError("Failed to generate a schedule.")
    return best_results, best_pair_counts


def duplicate_summary(players: list[str], pair_counts: dict[tuple[str, str], int]) -> tuple[dict[str, int], dict[str, list[tuple[str, int]]]]:
    dup_totals = {p: 0 for p in players}
    dup_detail: dict[str, list[tuple[str, int]]] = {p: [] for p in players}

    for (a, b), count in pair_counts.items():
        if count > 1:
            extra = count - 1
            dup_totals[a] += extra
            dup_totals[b] += extra
            dup_detail[a].append((b, count))
            dup_detail[b].append((a, count))

    for player in players:
        dup_detail[player].sort(key=lambda x: (-x[1], x[0]))

    return dup_totals, dup_detail


def format_output(players: list[str], schedule: list[GameResult], pair_counts: dict[tuple[str, str], int]) -> str:
    lines: list[str] = []
    lines.append("cEDH Tournament Pod Assignments")
    lines.append("=" * 32)
    lines.append("")

    for i, game in enumerate(schedule, start=1):
        lines.append(f"Game {i}")
        for pod_idx, pod in enumerate(game.pods, start=1):
            lines.append(f"Pod {pod_idx}: {', '.join(sorted(pod))}")
        if game.byes:
            lines.append(f"Byes: {', '.join(game.byes)}")
        else:
            lines.append("Byes: None")
        lines.append("")

    dup_totals, dup_detail = duplicate_summary(players, pair_counts)
    lines.append("Duplicate Matchups Per Player")
    lines.append("=" * 30)
    for player in sorted(players):
        lines.append(f"{player}: {dup_totals[player]}")
        if dup_detail[player]:
            parts = [f"{opponent} ({count}x)" for opponent, count in dup_detail[player]]
            lines.append(f"  Repeats with: {', '.join(parts)}")
        else:
            lines.append("  Repeats with: None")

    max_pair_meet = max(pair_counts.values(), default=0)
    lines.append("")
    lines.append(f"Highest times any two players met: {max_pair_meet}")
    lines.append("Target: no pair should exceed 2 if possible.")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate cEDH tournament pods.")
    parser.add_argument("-i", "--input", default="tournament_input.txt", help="Input file path")
    parser.add_argument("-o", "--output", default="tournament_output.gsheet", help="Output file path")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed for reproducible results")
    parser.add_argument(
        "--max-pair-meet",
        type=int,
        default=2,
        help="Target cap for how many times any two players should meet",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Process count for parallel schedule attempts (1 disables parallelism)",
    )
    parser.add_argument(
        "--strict-cap",
        action="store_true",
        help="Do not auto-relax the cap even if mathematically impossible",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    games, players = parse_input(input_path)
    config = tune_search(len(players), games)

    feasible_cap = minimum_feasible_cap(len(players), games)
    target_cap = args.max_pair_meet
    if not args.strict_cap and target_cap < feasible_cap:
        target_cap = feasible_cap
        print(
            f"Adjusted --max-pair-meet from {args.max_pair_meet} to {target_cap} "
            f"(minimum feasible for {len(players)} players x {games} games)."
        )

    workers = args.parallel_workers
    if workers < 1:
        workers = max(1, (os.cpu_count() or 1) - 1)

    schedule, pair_counts = generate_schedule(
        games,
        players,
        seed=args.seed,
        max_pair_meet=target_cap,
        parallel_workers=workers,
        config=config,
    )
    rendered = format_output(players, schedule, pair_counts)
    output_path.write_text(rendered, encoding="utf-8")

    over_cap, dupes = schedule_score(pair_counts, target_cap)
    print(
        f"Wrote {output_path} with {games} game(s) for {len(players)} player(s). "
        f"cap={target_cap}, over_cap={over_cap}, duplicate_events={dupes}."
    )


if __name__ == "__main__":
    main()
