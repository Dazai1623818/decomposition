package decomposition.cli;

import decomposition.DecompositionOptions.Mode;
import java.util.Objects;
import java.util.Set;

record CliOptions(
        String queryText,
        String queryFile,
        String exampleName,
        Set<String> freeVariables,
        Mode mode,
        int maxPartitions,
        int maxCovers,
        long timeBudgetMs,
        int enumerationLimit,
        boolean showVisualization,
        String outputPath) {

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

    boolean hasQueryFile() {
        return queryFile != null && !queryFile.isBlank();
    }

    boolean hasExample() {
        return exampleName != null && !exampleName.isBlank();
    }
}
