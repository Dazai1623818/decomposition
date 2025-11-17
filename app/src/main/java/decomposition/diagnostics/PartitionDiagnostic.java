package decomposition.diagnostics;

import java.util.Map;
import java.util.Objects;

/** Structured diagnostic entry describing why a partition or component was rejected. */
public record PartitionDiagnostic(
    Integer partitionIndex,
    Integer componentIndex,
    PartitionDiagnosticReason reason,
    Map<String, Object> attributes) {

  public static final String ATTR_FREE_VARIABLE = "freeVariable";
  public static final String ATTR_MAX_JOIN_NODES = "maxJoinNodes";
  public static final String ATTR_JOIN_NODE_COUNT = "joinNodeCount";
  public static final String ATTR_SIGNATURE = "signature";

  public PartitionDiagnostic {
    Objects.requireNonNull(reason, "reason");
    attributes = (attributes == null || attributes.isEmpty()) ? Map.of() : Map.copyOf(attributes);
  }

  public static PartitionDiagnostic freeVariableAbsent(int partitionIndex, String variable) {
    return new PartitionDiagnostic(
        partitionIndex,
        null,
        PartitionDiagnosticReason.FREE_VARIABLE_ABSENT,
        Map.of(ATTR_FREE_VARIABLE, variable));
  }

  public static PartitionDiagnostic excessJoinNodes(
      int partitionIndex, int componentIndex, int maxJoinNodes, int joinNodeCount) {
    return new PartitionDiagnostic(
        partitionIndex,
        componentIndex,
        PartitionDiagnosticReason.EXCESS_JOIN_NODES,
        Map.of(
            ATTR_MAX_JOIN_NODES, maxJoinNodes,
            ATTR_JOIN_NODE_COUNT, joinNodeCount));
  }

  public static PartitionDiagnostic missingComponentExpressions(
      Integer partitionIndex, int componentIndex, String signature) {
    return new PartitionDiagnostic(
        partitionIndex,
        componentIndex,
        PartitionDiagnosticReason.COMPONENT_EXPRESSIONS_MISSING,
        Map.of(ATTR_SIGNATURE, signature));
  }

  public static PartitionDiagnostic invalidComponentEndpoints(
      Integer partitionIndex, int componentIndex, String signature) {
    return new PartitionDiagnostic(
        partitionIndex,
        componentIndex,
        PartitionDiagnosticReason.COMPONENT_ENDPOINTS_INVALID,
        Map.of(ATTR_SIGNATURE, signature));
  }

  public static PartitionDiagnostic diagnosticsUnavailable(int partitionIndex) {
    return new PartitionDiagnostic(
        partitionIndex,
        null,
        PartitionDiagnosticReason.COMPONENT_DIAGNOSTICS_UNAVAILABLE,
        Map.of());
  }
}
