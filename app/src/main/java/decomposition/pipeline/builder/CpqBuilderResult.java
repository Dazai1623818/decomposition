package decomposition.pipeline.builder;

import decomposition.core.DecompositionResult;
import decomposition.cpq.model.CacheStats;
import decomposition.util.DecompositionPipelineUtils;
import java.util.Objects;

/**
 * Immutable aggregate of the builder execution containing the standard {@link DecompositionResult}
 * plus cache statistics.
 */
public record CpqBuilderResult(DecompositionResult result, CacheStats cacheStats) {

  public CpqBuilderResult {
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(cacheStats, "cacheStats");
  }

  public static CpqBuilderResult fromContext(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.extraction() == null) {
      throw new IllegalStateException("Extraction must complete before building the result.");
    }
    DecompositionResult result =
        DecompositionPipelineUtils.buildResult(
            context.extraction(),
            context.vertices(),
            context.partitions(),
            context.filteredPartitions(),
            context.cpqPartitions(),
            context.recognizedCatalogue(),
            context.finalExpression(),
            context.globalCatalogue(),
            context.partitionEvaluations(),
            context.diagnostics(),
            context.timing().elapsedMillis(),
            context.terminationReason());
    return new CpqBuilderResult(result, context.cacheStats());
  }
}
