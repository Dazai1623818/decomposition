package decomposition.pipeline.builder.diagnostics;

import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Partition;
import decomposition.pipeline.builder.CpqBuilderContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Performs optional deep verification of generated components against simple baselines (e.g.,
 * single-edge coverage) and records diagnostics for any mismatches.
 */
public final class VerificationService {

  public List<PartitionDiagnostic> verify(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    List<PartitionDiagnostic> issues = new ArrayList<>();

    if (context.partitionEvaluations().isEmpty()) {
      return issues;
    }

    for (PartitionEvaluation evaluation : context.partitionEvaluations()) {
      Partition partition = evaluation.partition();
      boolean singleEdgePartition =
          partition.components().stream().allMatch(component -> component.edgeCount() == 1);
      if (!singleEdgePartition) {
        continue;
      }

      boolean hasSingleEdgeExpression =
          context.recognizedCatalogue().stream().anyMatch(expr -> expr.edges().cardinality() == 1);
      if (!hasSingleEdgeExpression) {
        issues.add(
            PartitionDiagnostic.verificationFailed(
                evaluation.partitionIndex(),
                "Missing single-edge CPQ expression for simple partition"));
      }
    }

    return issues;
  }
}
