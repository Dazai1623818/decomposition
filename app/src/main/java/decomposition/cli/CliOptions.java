package decomposition.cli;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionOptions.Mode;
import decomposition.pipeline.PlanMode;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

record CliOptions(
    String queryFile,
    String exampleName,
    Set<String> freeVariables,
    Mode mode,
    int maxPartitions,
    long timeBudgetMs,
    int tupleLimit,
    PlanMode planMode,
    boolean evaluate,
    Path compareGraphPath,
    int indexK,
    boolean buildIndexOnly) {

  CliOptions {
    freeVariables = freeVariables == null ? Set.of() : Set.copyOf(freeVariables);
    Objects.requireNonNull(mode, "mode");
    planMode = planMode == null ? PlanMode.ALL : planMode;
    if (tupleLimit < 0) {
      throw new IllegalArgumentException("tupleLimit must be non-negative");
    }
    if (buildIndexOnly) {
      evaluate = true;
    }
    compareGraphPath = compareGraphPath == null ? Path.of("graph_huge.edge") : compareGraphPath;
    if (indexK < 1) {
      throw new IllegalArgumentException("index k must be at least 1");
    }
  }

  boolean hasQueryFile() {
    return queryFile != null && !queryFile.isBlank();
  }

  boolean hasExample() {
    return exampleName != null && !exampleName.isBlank();
  }

  static Builder builder() {
    return new Builder();
  }

  static final class Builder {
    private String queryFile;
    private String exampleName;
    private Set<String> freeVariables = Set.of();
    private Mode mode = DecompositionOptions.defaults().mode();
    private int maxPartitions = DecompositionOptions.defaults().maxPartitions();
    private long timeBudgetMs = DecompositionOptions.defaults().timeBudgetMs();
    private int tupleLimit = DecompositionOptions.defaults().tupleLimit();
    private PlanMode planMode = PlanMode.ALL;
    private boolean evaluate;
    private Path compareGraphPath = Path.of("graph_huge.edge");
    private int indexK = 3;
    private boolean buildIndexOnly;

    Builder queryFile(String queryFile) {
      this.queryFile = queryFile;
      return this;
    }

    Builder exampleName(String exampleName) {
      this.exampleName = exampleName;
      return this;
    }

    Builder freeVariables(Set<String> freeVariables) {
      if (freeVariables != null) {
        this.freeVariables = Set.copyOf(freeVariables);
      }
      return this;
    }

    Builder mode(Mode mode) {
      this.mode = mode;
      return this;
    }

    Builder maxPartitions(int maxPartitions) {
      this.maxPartitions = maxPartitions;
      return this;
    }

    Builder timeBudgetMs(long timeBudgetMs) {
      this.timeBudgetMs = timeBudgetMs;
      return this;
    }

    Builder tupleLimit(int tupleLimit) {
      this.tupleLimit = tupleLimit;
      return this;
    }

    Builder planMode(PlanMode planMode) {
      this.planMode = planMode;
      return this;
    }

    Builder evaluate(boolean evaluate) {
      this.evaluate = evaluate;
      return this;
    }

    Builder compareGraphPath(Path compareGraphPath) {
      this.compareGraphPath = compareGraphPath;
      return this;
    }

    Builder indexK(int indexK) {
      this.indexK = indexK;
      return this;
    }

    Builder buildIndexOnly(boolean buildIndexOnly) {
      this.buildIndexOnly = buildIndexOnly;
      return this;
    }

    CliOptions build() {
      int sources = 0;
      if (queryFile != null && !queryFile.isBlank()) sources++;
      if (exampleName != null && !exampleName.isBlank()) sources++;
      if (!buildIndexOnly && sources == 0) {
        throw new IllegalArgumentException("Provide exactly one of --file or --example");
      }
      if (sources > 1) {
        throw new IllegalArgumentException("Provide at most one of --file or --example");
      }

      boolean effectiveEvaluate = evaluate || buildIndexOnly;
      return new CliOptions(
          queryFile,
          exampleName,
          freeVariables,
          mode,
          maxPartitions,
          timeBudgetMs,
          tupleLimit,
          planMode,
          effectiveEvaluate,
          compareGraphPath,
          indexK,
          buildIndexOnly);
    }
  }
}
