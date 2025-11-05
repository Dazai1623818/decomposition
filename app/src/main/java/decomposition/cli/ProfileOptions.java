package decomposition.cli;

import decomposition.DecompositionOptions;
import decomposition.profile.PipelineProfiler.NamedQuery;
import java.util.List;
import java.util.Objects;

record ProfileOptions(List<NamedQuery> queries, DecompositionOptions options) {

  ProfileOptions {
    Objects.requireNonNull(queries, "queries");
    if (queries.isEmpty()) {
      throw new IllegalArgumentException("Profiling requires at least one query.");
    }
    queries = List.copyOf(queries);
    options = options != null ? options : DecompositionOptions.defaults();
  }
}
