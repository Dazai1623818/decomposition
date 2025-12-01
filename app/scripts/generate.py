from __future__ import annotations

import argparse
from pathlib import Path
import random


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "app" / "graphs"


def auto_name(num_vertices: int, num_edges: int, num_labels: int, seed: int) -> str:
    return f"graph_v{num_vertices}_e{num_edges}_l{num_labels}_s{seed}.edge"


def generate(
    path: Path,
    num_vertices: int,
    num_edges: int,
    num_labels: int,
    seed: int = 1,
    embed_example1: bool = True,
    embed_count: int = 1,
) -> None:
    rng = random.Random(seed)
    embedded_edges: list[tuple[int, int, int]] = []
    if embed_example1:
        if num_vertices < 4 or num_labels < 5:
            raise ValueError("Embedding example1 requires at least 4 vertices and 5 labels.")
        # Map example1 variables to vertices: A=0, B=1, C=2, D=3
        # Labels use 0-indexing: 0, 1, 2, 3, 4 (requires at least 5 labels)
        example1_pattern = [(0, 1, 1), (1, 2, 2), (2, 3, 3), (3, 0, 4), (0, 2, 5)]
        # Spread embeddings roughly evenly across the vertex space so they do not overlap at 0–3.
        stride = max(1, num_vertices // max(embed_count, 1))
        for idx in range(embed_count):
            offset = (idx * stride) % num_vertices
            for src, tgt, label in example1_pattern:
                embedded_edges.append(((src + offset) % num_vertices, (tgt + offset) % num_vertices, label))

    remaining_edges = num_edges - len(embedded_edges)
    if remaining_edges < 0:
        raise ValueError(f"num_edges must be at least {len(embedded_edges)} to embed example1.")

    with path.open("w", encoding="ascii") as fh:
        fh.write(f"{num_vertices} {num_edges} {num_labels}\n")
        for src, tgt, label in embedded_edges:
            fh.write(f"{src} {tgt} {label}\n")
        for edge_index in range(remaining_edges):
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
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for auto-named output files when --output is not provided",
    )
    parser.add_argument("--vertices", type=int, default=200_000, help="Number of vertices to include")
    parser.add_argument("--edges", type=int, default=1_000_000, help="Number of edges to generate")
    parser.add_argument("--labels", type=int, default=10, help="Number of distinct edge labels")
    parser.add_argument("--seed", type=int, help="RNG seed for reproducibility (default: random)")
    parser.add_argument(
        "--no-embed-example1",
        action="store_true",
        help="Skip embedding example1 edges into the generated graph",
    )
    parser.add_argument(
        "--embed-count",
        type=int,
        default=1,
        help="Number of times to embed example1 (default: 1)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**63 - 1)
    output_path = args.output or (args.output_dir / auto_name(args.vertices, args.edges, args.labels, seed))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate(
        output_path,
        args.vertices,
        args.edges,
        args.labels,
        seed,
        embed_example1=not args.no_embed_example1,
        embed_count=args.embed_count,
    )


if __name__ == "__main__":
    main()
