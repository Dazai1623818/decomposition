# Module layout

- `decomposition.core`: shared domain types (config, results) plus `core.model` and `core.diagnostics`.
- `decomposition.examples`: canned CQ builders and random example configuration.
- `decomposition.pipeline`: end-to-end pipeline logic. Key areas:
  - `builder`: pipeline orchestration, stages, and diagnostics.
  - `extract`: CQ-to-intermediate extraction.
  - `partitioning`: partition enumeration and filtering.
  - `generation`: component/global CPQ synthesis utilities.
- `decomposition.cpq`: CPQ expression builders and cache model.
- `decomposition.cli`: CLI entry points and command handlers.
- `decomposition.eval`: native evaluator integration and comparison tooling.
- `decomposition.util`: shared helpers for graph/analysis utilities.
- `decomposition.tools`: standalone utilities (e.g., `PlotCPQ` Swing visualizer).
- `decomposition.profile`: profiling helpers for the pipeline.
- `decomposition.nativeindex`: native index bindings used by evaluation flows.
