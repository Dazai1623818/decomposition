package decomposition.util;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
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
      List<PartitionDiagnostic> diagnostics,
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
