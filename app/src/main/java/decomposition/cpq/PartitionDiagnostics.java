package decomposition.cpq;

import decomposition.cpq.model.ComponentCPQExpressions;
import java.util.ArrayList;
import java.util.List;

/** Tracks diagnostics for the most recent partition analysis. */
final class PartitionDiagnostics {
  private final List<String> currentFailures = new ArrayList<>();
  private volatile List<String> lastComponentDiagnostics = List.of();

  void beginPartition() {
    currentFailures.clear();
  }

  void recordComponent(
      int componentIndex, String signature, ComponentCPQExpressions rules, boolean joinNodesEmpty) {
    String message = buildFailureDiagnostic(componentIndex, signature, rules, joinNodesEmpty);
    if (message != null) {
      currentFailures.add(message);
    }
  }

  void failPartition() {
    lastComponentDiagnostics = currentFailures.isEmpty() ? List.of() : List.copyOf(currentFailures);
  }

  void succeedPartition() {
    lastComponentDiagnostics = List.of();
  }

  List<String> lastComponentDiagnostics() {
    return lastComponentDiagnostics;
  }

  private String buildFailureDiagnostic(
      int componentIndex, String signature, ComponentCPQExpressions rules, boolean joinNodesEmpty) {
    if (rules == null) {
      return null;
    }
    if (rules.rawRules().isEmpty()) {
      return "Partition component#"
          + componentIndex
          + " rejected: no CPQ construction rules for bits "
          + signature;
    }
    if (!joinNodesEmpty && rules.joinFilteredRules().isEmpty()) {
      return "Partition component#"
          + componentIndex
          + " rejected: endpoints not on join nodes for bits "
          + signature;
    }
    return null;
  }
}
