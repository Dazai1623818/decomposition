from __future__ import annotations

import argparse
from pathlib import Path
import random


def auto_name(num_vertices: int, num_edges: int, num_labels: int, seed: int) -> str:
    return f"graph_v{num_vertices}_e{num_edges}_l{num_labels}_s{seed}.edge"


def generate(path: Path, num_vertices: int, num_edges: int, num_labels: int, seed: int = 1) -> None:
    rng = random.Random(seed)
    with path.open("w", encoding="ascii") as fh:
        fh.write(f"{num_vertices} {num_edges} {num_labels}\n")
        for edge_index in range(num_edges):
            src = edge_index % num_vertices
            tgt = (edge_index * 17 + 23) % num_vertices
            if tgt == src:
                tgt = (tgt + 1) % num_vertices
            label = rng.randrange(num_labels)
            fh.write(f"{src} {tgt} {label}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a synthetic edge list graph.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output .edge file path (defaults to auto-named file based on parameters)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory for auto-named output files when --output is not provided",
    )
    parser.add_argument("--vertices", type=int, default=200_000, help="Number of vertices to include")
    parser.add_argument("--edges", type=int, default=1_000_000, help="Number of edges to generate")
    parser.add_argument("--labels", type=int, default=10, help="Number of distinct edge labels")
    parser.add_argument("--seed", type=int, help="RNG seed for reproducibility (default: random)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**63 - 1)
    output_path = args.output or (args.output_dir / auto_name(args.vertices, args.edges, args.labels, seed))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate(output_path, args.vertices, args.edges, args.labels, seed)


if __name__ == "__main__":
    main()
