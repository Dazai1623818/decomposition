package decomposition.diagnostics;

/** Enumerates structured reasons why a partition or component was rejected. */
public enum PartitionDiagnosticReason {
  FREE_VARIABLE_ABSENT,
  EXCESS_JOIN_NODES,
  COMPONENT_EXPRESSIONS_MISSING,
  COMPONENT_ENDPOINTS_INVALID,
  COMPONENT_DIAGNOSTICS_UNAVAILABLE,
  VERIFICATION_FAILED;
}
