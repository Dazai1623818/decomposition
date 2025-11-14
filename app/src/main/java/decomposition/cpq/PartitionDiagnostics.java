package decomposition.cpq;

import decomposition.model.Component;
import decomposition.partitions.FilteredPartition;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Tracks diagnostics for the most recent partition analysis. */
public final class PartitionDiagnostics {
  private final List<String> currentFailures = new ArrayList<>();
  private volatile List<String> lastComponentDiagnostics = List.of();

  public void beginPartition() {
    currentFailures.clear();
  }

  public void recordComponent(
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
    String message =
        buildFailureDiagnostic(
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

  public List<String> lastComponentDiagnostics() {
    return lastComponentDiagnostics;
  }

  private String buildFailureDiagnostic(
      int componentIndex,
      Component component,
      String signature,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      Set<String> localJoinNodes,
      List<CPQExpression> diagnosticCandidates) {
    if (!hasRawExpressions) {
      return "Partition component#"
          + componentIndex
          + " rejected: no CPQ expressions for bits "
          + signature;
    }
    if (!localJoinNodes.isEmpty() && !hasJoinFilteredExpressions) {
      boolean anyRespect =
          diagnosticCandidates.stream()
              .anyMatch(
                  rule ->
                      JoinNodeUtils.endpointsRespectJoinNodeRoles(rule, component, localJoinNodes));
      if (!anyRespect) {
        return "Partition component#"
            + componentIndex
            + " rejected: endpoints not on join nodes for bits "
            + signature;
      }
    }
    return null;
  }
}
