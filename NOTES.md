# CPQ Native Index Integration Notes

- The project now pulls the native index from Maven (`dev.roanh.cpqnativeindex:cpq-native-index:1.0`) and relies on gMark `2.1` for the query DSL. The native index jar itself was compiled against the **legacy** gMark packages (`dev.roanh.gmark.util.UniqueGraph`, `dev.roanh.gmark.core.graph.Predicate`, `dev.roanh.gmark.util.RangeList`, `dev.roanh.gmark.util.IDable`), which no longer exist in the modern artifact.

- To keep the evaluator working without bundling old jars, minimal compatibility shims live under `app/src/main/java/dev/roanh/gmark/...`. They reproduce just enough of the legacy API for the index to parse graphs:
  - `dev.roanh.gmark.core.graph.Predicate`
  - `dev.roanh.gmark.util.IDable`
  - `dev.roanh.gmark.util.RangeList`
  - `dev.roanh.gmark.util.UniqueGraph`
- `decomposition.eval.GraphLoader` replaces the old `IndexUtil.readGraph` so we can build the compatibility graph before handing it to `dev.roanh.cpqindex.Index`.

## Follow-up work needed upstream
1. In the native index repository, update the build to depend on the modern gMark artifact (`dev.roanh.gmark:gmark:2.1` or newer) and switch imports from the legacy packages to the current ones (e.g., `dev.roanh.gmark.util.graph.generic.UniqueGraph`, `dev.roanh.gmark.type.schema.Predicate`, etc.).
2. Publish the updated native index so this repo can drop the compatibility layer and the extra source files under `dev/roanh/gmark/**`.
3. Once the new release exists, remove the shim classes, delete `decomposition.eval.GraphLoader`, and go back to consuming the official gMark types directly.

Until those updates land upstream, keep the note that the shim classes are intentional and should not be deleted.
