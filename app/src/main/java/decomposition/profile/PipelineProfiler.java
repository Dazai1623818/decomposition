package decomposition.profile;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.examples.Example;
import decomposition.examples.RandomExampleConfig;
import decomposition.pipeline.Pipeline;
import decomposition.pipeline.PlanMode;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Utility for profiling the decomposition pipeline across multiple queries. */
public final class PipelineProfiler {

  /** Named query input for profiling runs. */
  public record NamedQuery(String name, CQ query, Set<String> freeVariables) {
    public NamedQuery {
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(query, "query");
      freeVariables = freeVariables == null ? Set.of() : Set.copyOf(freeVariables);
    }

    public static NamedQuery of(String name, CQ query) {
      return new NamedQuery(name, query, Set.of());
    }
  }

  /** Per-query profiling outcome. */
  public record ProfileRun(
      String queryName,
      long elapsedMillis,
      int totalPartitions,
      int filteredPartitions,
      int validPartitions,
      int recognizedComponents,
      String terminationReason) {}

  /** Aggregated profiling report. */
  public record PipelineProfile(List<ProfileRun> runs) {
    public PipelineProfile {
      runs = List.copyOf(Objects.requireNonNull(runs, "runs"));
    }

    public long totalElapsedMillis() {
      return runs.stream().mapToLong(ProfileRun::elapsedMillis).sum();
    }
  }

  public PipelineProfile profile(List<NamedQuery> queries, DecompositionOptions options) {
    Objects.requireNonNull(queries, "queries");
    if (queries.isEmpty()) {
      throw new IllegalArgumentException("Profiling requires at least one query.");
    }
    DecompositionOptions effective = options != null ? options : DecompositionOptions.defaults();

    List<ProfileRun> runs = new ArrayList<>(queries.size());
    Pipeline pipeline = new Pipeline();
    for (NamedQuery query : queries) {
      DecompositionResult result =
          pipeline.decompose(query.query(), query.freeVariables(), effective, PlanMode.ALL);
      runs.add(
          new ProfileRun(
              query.name(),
              result.elapsedMillis(),
              result.totalPartitions(),
              result.filteredPartitions(),
              result.cpqPartitions().size(),
              result.recognizedCatalogue().size(),
              result.terminationReason()));
    }
    return new PipelineProfile(runs);
  }

  public static List<NamedQuery> defaultExamples() {
    return List.of(
        NamedQuery.of("example1", Example.example1()),
        NamedQuery.of("example2", Example.example2()),
        NamedQuery.of("example3", Example.example3()),
        NamedQuery.of("example4", Example.example4()),
        NamedQuery.of("example5", Example.example5()),
        NamedQuery.of("example6", Example.example6()),
        NamedQuery.of("example7", Example.example7()),
        NamedQuery.of("example8", Example.example8()));
  }

  public static NamedQuery randomQuery(String name, RandomExampleConfig config) {
    RandomExampleConfig effective = config != null ? config : RandomExampleConfig.defaults();
    return NamedQuery.of(name, Example.random(effective));
  }
}
