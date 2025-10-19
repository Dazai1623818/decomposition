package decomposition.cli;

import decomposition.DecompositionOptions.Mode;
import java.util.Objects;
import java.util.Set;

record CliOptions(
        String queryText,
        String queryFile,
        Set<String> freeVariables,
        Mode mode,
        int maxPartitions,
        int maxCovers,
        long timeBudgetMs,
        boolean showVisualization,
        String outputPath) {

    CliOptions {
        freeVariables = freeVariables == null ? Set.of() : Set.copyOf(freeVariables);
        Objects.requireNonNull(mode, "mode");
    }

    boolean hasQueryText() {
        return queryText != null && !queryText.isBlank();
    }

    boolean hasQueryFile() {
        return queryFile != null && !queryFile.isBlank();
    }
}
