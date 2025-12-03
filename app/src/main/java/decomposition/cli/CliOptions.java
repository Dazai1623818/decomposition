package decomposition.cli;

import decomposition.core.DecompositionOptions.Mode;
import decomposition.examples.RandomExampleConfig;
import decomposition.pipeline.PlanMode;
import java.nio.file.Path;
import java.util.List;
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
    boolean showVisualization,
    String outputPath,
    RandomExampleConfig randomExampleConfig,
    boolean compareWithIndex,
    Path compareGraphPath,
    List<Path> compareDecompositions,
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
      compareWithIndex = true;
    }
    compareGraphPath = compareGraphPath == null ? Path.of("graph_huge.edge") : compareGraphPath;
    compareDecompositions =
        compareDecompositions == null ? List.of() : List.copyOf(compareDecompositions);
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
}
