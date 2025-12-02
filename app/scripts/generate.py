from __future__ import annotations

import argparse
from pathlib import Path
import random


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "app" / "graphs"

# Embedding templates aligned with the Java examples in decomposition.examples.Example.
EMBED_PATTERNS: dict[str, dict[str, object]] = {
    "example1": {
        "min_vertices": 4,
        "min_labels": 6,
        "edges": [(0, 1, 1), (1, 2, 2), (2, 3, 3), (3, 0, 4), (0, 2, 5)],
    },
    "example2": {
        "min_vertices": 3,
        "min_labels": 3,
        "edges": [(0, 1, 1), (1, 2, 2)],
    },
    "example3": {
        "min_vertices": 4,
        "min_labels": 7,
        "edges": [(0, 1, 1), (1, 2, 2), (2, 3, 3), (3, 0, 4), (0, 2, 5), (0, 0, 6)],
    },
    "example4": {
        "min_vertices": 4,
        "min_labels": 8,
        "edges": [
            (0, 1, 1),
            (1, 2, 2),
            (2, 3, 3),
            (3, 0, 4),
            (0, 2, 5),
            (0, 0, 6),
            (1, 3, 7),
        ],
    },
    "example5": {
        "min_vertices": 4,
        "min_labels": 7,
        "edges": [(0, 1, 3), (1, 2, 4), (1, 3, 5), (2, 3, 6)],
    },
    "example6": {
        "min_vertices": 6,
        "min_labels": 6,
        "edges": [(0, 1, 1), (1, 0, 1), (1, 2, 2), (2, 3, 3), (3, 4, 4), (3, 5, 5)],
    },
    "example7": {
        "min_vertices": 3,
        "min_labels": 8,
        "edges": [
            (0, 1, 1),
            (0, 1, 2),
            (1, 2, 3),
            (1, 2, 4),
            (2, 0, 5),
            (2, 0, 6),
            (2, 2, 7),
        ],
    },
    "example8": {
        "min_vertices": 5,
        "min_labels": 9,
        "edges": [(0, 1, 1), (0, 1, 2), (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 0, 6), (4, 0, 7), (2, 0, 8)],
    },
}


def auto_name(num_vertices: int, num_edges: int, num_labels: int, seed: int) -> str:
    return f"graph_v{num_vertices}_e{num_edges}_l{num_labels}_s{seed}.edge"


def generate(
    path: Path,
    num_vertices: int,
    num_edges: int,
    num_labels: int,
    seed: int = 1,
    embed_count: int = 1,
    embeddings: list[str] | None = None,
) -> None:
    embed_names = embeddings if embeddings is not None else ["example1"]
    rng = random.Random(seed)
    embedded_edges: list[tuple[int, int, int]] = []

    if embed_count < 0:
        raise ValueError("embed_count cannot be negative.")
    if not embed_names:
        embed_count = 0

    unknown = [name for name in embed_names if name not in EMBED_PATTERNS]
    if unknown:
        raise ValueError(f"Unknown embeddings requested: {', '.join(unknown)}")

    total_embeds = len(embed_names) * embed_count
    stride = max(1, num_vertices // max(total_embeds, 1))
    placement_index = 0

    for embed_name in embed_names:
        pattern = EMBED_PATTERNS[embed_name]
        min_vertices = int(pattern["min_vertices"])
        min_labels = int(pattern["min_labels"])
        if num_vertices < min_vertices or num_labels < min_labels:
            raise ValueError(
                f"Embedding {embed_name} requires at least {min_vertices} vertices and {min_labels} labels."
            )
        for _ in range(embed_count):
            offset = (placement_index * stride) % num_vertices
            placement_index += 1
            for src, tgt, label in pattern["edges"]:
                embedded_edges.append(((src + offset) % num_vertices, (tgt + offset) % num_vertices, label))

    remaining_edges = num_edges - len(embedded_edges)
    if remaining_edges < 0:
        raise ValueError(f"num_edges must be at least {len(embedded_edges)} to embed the requested patterns.")

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
        "--embed",
        dest="embeddings",
        choices=sorted(EMBED_PATTERNS.keys()),
        action="append",
        help="Embedding pattern(s) to insert. Repeat to include multiple. Defaults to example1 unless disabled.",
    )
    parser.add_argument(
        "--embed-count",
        type=int,
        default=1,
        help="Number of times to embed each selected pattern (default: 1)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**63 - 1)
    output_path = args.output or (args.output_dir / auto_name(args.vertices, args.edges, args.labels, seed))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.embeddings:
        embeddings = args.embeddings
        if args.no_embed_example1:
            embeddings = [name for name in embeddings if name != "example1"]
    elif args.no_embed_example1:
        embeddings = []
    else:
        embeddings = ["example1"]

    generate(
        output_path,
        args.vertices,
        args.edges,
        args.labels,
        seed,
        embed_count=args.embed_count,
        embeddings=embeddings,
    )


if __name__ == "__main__":
    main()
