#!/usr/bin/env python3
"""Generate a synthetic gMark-style edge list for evaluation."""

from __future__ import annotations

import argparse
import random
from pathlib import Path

EXAMPLE_EDGES = [
    # example1
    (0, 1, 1),
    (1, 2, 2),
    (2, 3, 3),
    (3, 0, 4),
    (0, 2, 5),
    # example2
    (10, 11, 1),
    (11, 12, 2),
    (12, 13, 3),
    (13, 10, 4),
    (10, 12, 5),
    # example3
    (20, 21, 1),
    (21, 22, 2),
    (22, 23, 3),
    (23, 20, 4),
    (20, 22, 5),
    (20, 20, 6),
    # example4
    (30, 31, 1),
    (31, 32, 2),
    (32, 33, 3),
    (33, 30, 4),
    (30, 32, 5),
    (30, 30, 6),
    (31, 33, 7),
    # example5
    (40, 41, 3),
    (41, 42, 4),
    (41, 43, 5),
    (42, 43, 6),
    # example6
    (50, 51, 1),
    (51, 50, 1),
    (51, 52, 2),
    (52, 53, 3),
    (53, 54, 4),
    (53, 55, 5),
    # example7
    (60, 61, 1),
    (60, 61, 2),
    (61, 62, 3),
    (61, 62, 4),
    (62, 60, 5),
    (62, 60, 6),
    (62, 62, 7),
    # example8
    (70, 71, 1),
    (70, 71, 2),
    (71, 72, 3),
    (72, 73, 4),
    (73, 74, 5),
    (74, 70, 6),
    (74, 70, 7),
    (72, 70, 8),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a large .edge graph file")
    parser.add_argument(
        "--nodes", type=int, default=100_000, help="number of nodes in the graph"
    )
    parser.add_argument(
        "--edges", type=int, default=100_000, help="number of edges to emit"
    )
    parser.add_argument(
        "--labels", type=int, default=256, help="number of predicate labels"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("app/graph_huge.edge"),
        help="path to write the edge list",
    )
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    required_nodes = max(src for src, _, _ in EXAMPLE_EDGES) + 1
    if args.nodes < required_nodes:
        raise SystemExit(
            f"--nodes must be at least {required_nodes} to include the example subgraphs"
        )
    if args.edges < len(EXAMPLE_EDGES):
        raise SystemExit(
            f"--edges must be at least {len(EXAMPLE_EDGES)} to keep every example edge"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as out:
        out.write(f"{args.nodes} {args.edges} {args.labels}\n")
        for src, tgt, label in EXAMPLE_EDGES:
            out.write(f"{src} {tgt} {label}\n")

        remaining = args.edges - len(EXAMPLE_EDGES)
        for _ in range(remaining):
            src = random.randrange(args.nodes)
            tgt = random.randrange(args.nodes)
            label = random.randrange(args.labels)
            out.write(f"{src} {tgt} {label}\n")


if __name__ == "__main__":
    main()
