package decomposition.cli;

import decomposition.DecompositionOptions.Mode;
import decomposition.RandomExampleConfig;
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
    RandomExampleConfig randomExampleConfig) {

  CliOptions {
    freeVariables = freeVariables == null ? Set.of() : Set.copyOf(freeVariables);
    Objects.requireNonNull(mode, "mode");
    if (enumerationLimit < 0) {
      throw new IllegalArgumentException("enumerationLimit must be non-negative");
    }
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
