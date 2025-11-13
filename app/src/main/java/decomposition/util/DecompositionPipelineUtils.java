package decomposition.util;

import decomposition.DecompositionOptions;
import decomposition.DecompositionResult;
import decomposition.PartitionEvaluation;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Partition;
import decomposition.pipeline.DecompositionPipelineState.PartitionSets;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import decomposition.util.Timing;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Shared helpers that keep the pipeline class focused on orchestration. */
public final class DecompositionPipelineUtils {
  private DecompositionPipelineUtils() {}

  public static DecompositionOptions resolveOptions(DecompositionOptions options) {
    return (options != null) ? options : DecompositionOptions.defaults();
  }

  public static int hashVarContext(Map<String, String> originalVarMap) {
    if (originalVarMap == null || originalVarMap.isEmpty()) {
      return 0;
    }
    int hash = 1;
    for (Map.Entry<String, String> entry : originalVarMap.entrySet()) {
      hash = 31 * hash + Objects.hash(entry.getKey(), entry.getValue());
    }
    return hash;
  }

  public static boolean overBudget(DecompositionOptions opts, Timing timing) {
    return opts.timeBudgetMs() > 0 && timing.elapsedMillis() > opts.timeBudgetMs();
  }

  public static boolean overBudget(DecompositionOptions opts, long elapsedMillis) {
    return opts.timeBudgetMs() > 0 && elapsedMillis > opts.timeBudgetMs();
  }

  public static void addComponentDiagnostics(
      List<String> diagnostics, PartitionDiagnostics partitionDiagnostics) {
    List<String> cached = partitionDiagnostics.lastComponentDiagnostics();
    if (cached != null && !cached.isEmpty()) {
      diagnostics.addAll(cached);
    } else {
      diagnostics.add("Partition rejected but component diagnostics were unavailable.");
    }
  }

  public static CPQExpression selectPreferredFinalComponent(List<CPQExpression> rules) {
    return (rules == null || rules.isEmpty()) ? null : rules.get(0);
  }

  public static <T> List<List<T>> enumerateTuples(List<List<T>> perComponent, int limit) {
    if (perComponent == null || perComponent.isEmpty()) {
      return List.of();
    }

    int n = perComponent.size();
    int[] idx = new int[n];
    List<List<T>> out = new ArrayList<>();
    while (true) {
      List<T> tuple = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        tuple.add(perComponent.get(i).get(idx[i]));
      }
      out.add(tuple);
      if (limit > 0 && out.size() >= limit) {
        break;
      }

      int p = n - 1;
      while (p >= 0) {
        idx[p]++;
        if (idx[p] < perComponent.get(p).size()) {
          break;
        }
        idx[p] = 0;
        p--;
      }
      if (p < 0) {
        break;
      }
    }
    return out;
  }

  public static DecompositionResult earlyExitAfterPartitioning(
      PipelineContext ctx, PartitionSets parts, Timing timing) {
    return buildResult(
        ctx.extraction(),
        ctx.vertices(),
        parts.partitions(),
        parts.filtered(),
        List.of(),
        List.of(),
        null,
        List.of(),
        List.of(),
        parts.diagnostics(),
        timing.elapsedMillis(),
        "time_budget_exceeded_after_partitioning");
  }

  public static DecompositionResult buildResult(
      ExtractionResult extraction,
      Set<String> vertices,
      List<Partition> partitions,
      List<Partition> filteredPartitions,
      List<Partition> cpqPartitions,
      List<CPQExpression> recognizedCatalogue,
      CPQExpression finalExpression,
      List<CPQExpression> globalCatalogue,
      List<PartitionEvaluation> partitionEvaluations,
      List<String> diagnostics,
      long elapsedMillis,
      String terminationReason) {

    return new DecompositionResult(
        extraction.edges(),
        extraction.freeVariables(),
        vertices.size(),
        partitions.size(),
        filteredPartitions.size(),
        partitions,
        filteredPartitions,
        (cpqPartitions != null) ? cpqPartitions : List.of(),
        (recognizedCatalogue != null) ? recognizedCatalogue : List.of(),
        finalExpression,
        (globalCatalogue != null) ? globalCatalogue : List.of(),
        (partitionEvaluations != null) ? partitionEvaluations : List.of(),
        diagnostics,
        elapsedMillis,
        terminationReason);
  }
}
