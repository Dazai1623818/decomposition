package decomposition.cpq;

import java.util.ArrayList;
import java.util.List;

/** Tracks diagnostics for the most recent partition analysis. */
public final class PartitionDiagnostics {
  private final List<String> currentFailures = new ArrayList<>();
  private volatile List<String> lastComponentDiagnostics = List.of();

  public void beginPartition() {
    currentFailures.clear();
  }

  public void recordComponent(
      int componentIndex,
      String signature,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      boolean joinNodesEmpty) {
    String message =
        buildFailureDiagnostic(
            componentIndex,
            signature,
            hasRawExpressions,
            hasJoinFilteredExpressions,
            joinNodesEmpty);
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
      String signature,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      boolean joinNodesEmpty) {
    if (!hasRawExpressions) {
      return "Partition component#"
          + componentIndex
          + " rejected: no CPQ expressions for bits "
          + signature;
    }
    if (!joinNodesEmpty && !hasJoinFilteredExpressions) {
      return "Partition component#"
          + componentIndex
          + " rejected: endpoints not on join nodes for bits "
          + signature;
    }
    return null;
  }
}
