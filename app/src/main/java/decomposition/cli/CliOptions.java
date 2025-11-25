package decomposition.cli;

import decomposition.core.DecompositionOptions.Mode;
import decomposition.examples.RandomExampleConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;

record CliOptions(
    String queryText,
    String cpqExpression,
    String queryFile,
    String exampleName,
    Set<String> freeVariables,
    Mode mode,
    int maxPartitions,
    long timeBudgetMs,
    int enumerationLimit,
    boolean singleTuplePerPartition,
    boolean showVisualization,
    String outputPath,
    RandomExampleConfig randomExampleConfig,
    boolean compareWithIndex,
    Path compareGraphPath,
    List<Path> compareDecompositions,
    boolean buildIndexOnly) {

  CliOptions {
    freeVariables = freeVariables == null ? Set.of() : Set.copyOf(freeVariables);
    Objects.requireNonNull(mode, "mode");
    if (enumerationLimit < 0) {
      throw new IllegalArgumentException("enumerationLimit must be non-negative");
    }
    if (buildIndexOnly) {
      compareWithIndex = true;
    }
    compareGraphPath = compareGraphPath == null ? Path.of("graph_huge.edge") : compareGraphPath;
    compareDecompositions =
        compareDecompositions == null ? List.of() : List.copyOf(compareDecompositions);
  }

  boolean hasQueryText() {
    return queryText != null && !queryText.isBlank();
  }

  boolean hasCpqExpression() {
    return cpqExpression != null && !cpqExpression.isBlank();
  }

  boolean hasQueryFile() {
    return queryFile != null && !queryFile.isBlank();
  }

  boolean hasExample() {
    return exampleName != null && !exampleName.isBlank();
  }
}
