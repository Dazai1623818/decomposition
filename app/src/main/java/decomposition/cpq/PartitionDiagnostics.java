package decomposition.cpq;

import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Component;
import decomposition.pipeline.partitioning.FilteredPartition;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Tracks diagnostics for the most recent partition analysis. */
public final class PartitionDiagnostics {
  private final List<PartitionDiagnostic> currentFailures = new ArrayList<>();
  private volatile List<PartitionDiagnostic> lastComponentDiagnostics = List.of();

  public void beginPartition() {
    currentFailures.clear();
  }

  public void recordComponent(
      int partitionIndex,
      int componentIndex,
      Component component,
      FilteredPartition filteredPartition,
      String signature,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      List<CPQExpression> diagnosticCandidates) {
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(filteredPartition, "filteredPartition");
    Objects.requireNonNull(diagnosticCandidates, "diagnosticCandidates");

    Set<String> localJoinNodes = filteredPartition.joinNodesForComponent(component);
    PartitionDiagnostic message =
        buildFailureDiagnostic(
            partitionIndex,
            componentIndex,
            component,
            signature,
            hasRawExpressions,
            hasJoinFilteredExpressions,
            localJoinNodes,
            diagnosticCandidates);
    if (message != null) {
      currentFailures.add(message);
    }
  }

  public void failPartition() {
    lastComponentDiagnostics = currentFailures.isEmpty() ? List.of() : List.copyOf(currentFailures);
  }

  public void succeedPartition() {
    lastComponentDiagnostics = List.of();
  }

  public List<PartitionDiagnostic> lastComponentDiagnostics() {
    return lastComponentDiagnostics;
  }

  private PartitionDiagnostic buildFailureDiagnostic(
      int partitionIndex,
      int componentIndex,
      Component component,
      String signature,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      Set<String> localJoinNodes,
      List<CPQExpression> diagnosticCandidates) {
    if (!hasRawExpressions) {
      return PartitionDiagnostic.missingComponentExpressions(
          partitionIndex, componentIndex, signature);
    }
    if (!localJoinNodes.isEmpty() && !hasJoinFilteredExpressions) {
      boolean anyRespect =
          diagnosticCandidates.stream()
              .anyMatch(
                  rule ->
                      JoinNodeUtils.endpointsRespectJoinNodeRoles(rule, component, localJoinNodes));
      if (!anyRespect) {
        return PartitionDiagnostic.invalidComponentEndpoints(
            partitionIndex, componentIndex, signature);
      }
    }
    return null;
  }
}
