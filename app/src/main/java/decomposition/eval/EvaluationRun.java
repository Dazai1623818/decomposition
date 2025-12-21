package decomposition.eval;

import decomposition.cpq.CPQExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Tracks a single end-to-end run for decomposition (and optional evaluation).
 *
 * <p>Stores the decompositions plus timing/count data for each phase.
 */
public final class EvaluationRun {

  public enum Phase {
    COMPONENT_ENUMERATION,
    PARTITION_GENERATION,
    EXPRESSION_BUILDING,
    DEDUPLICATION,
    INDEX_LOAD,
    INDEX_BUILD,
    CPQ_EVALUATION,
    JOIN,
    TOTAL
  }

  private final long[] phaseMs = new long[Phase.values().length];
  private final int[] phaseCounts = new int[Phase.values().length];

  private long expressionBuildingAccumMs;

  private List<List<CPQExpression>> decompositions = List.of();
  private List<Map<String, Integer>> evaluationResults = List.of();
  private final List<String> events = new ArrayList<>();

  public EvaluationRun() {}

  // ----- Recording -----

  public void recordPhase(Phase phase, long ms, int count) {
    phaseMs[phase.ordinal()] = ms;
    phaseCounts[phase.ordinal()] = count;
  }

  public void recordPhaseMs(Phase phase, long ms) {
    phaseMs[phase.ordinal()] = ms;
  }

  public void recordPhaseCount(Phase phase, int count) {
    phaseCounts[phase.ordinal()] = count;
  }

  public void addExpressionBuildingTime(long ms) {
    expressionBuildingAccumMs += ms;
  }

  public void finalizeExpressionBuilding() {
    recordPhaseMs(Phase.EXPRESSION_BUILDING, expressionBuildingAccumMs);
  }

  public void setDecompositions(List<List<CPQExpression>> decompositions) {
    this.decompositions = List.copyOf(Objects.requireNonNull(decompositions, "decompositions"));
  }

  public void setEvaluationResults(List<Map<String, Integer>> evaluationResults) {
    this.evaluationResults =
        List.copyOf(Objects.requireNonNull(evaluationResults, "evaluationResults"));
  }

  public void logEvent(String message) {
    if (message == null || message.isBlank()) {
      return;
    }
    events.add(message);
  }

  // ----- Accessors -----

  public List<List<CPQExpression>> decompositions() {
    return decompositions;
  }

  public List<Map<String, Integer>> evaluationResults() {
    return evaluationResults;
  }

  public List<String> events() {
    return List.copyOf(events);
  }

  public int size() {
    return decompositions.size();
  }

  public boolean isEmpty() {
    return decompositions.isEmpty();
  }

  // ----- Convenience metrics -----

  public long componentEnumerationMs() {
    return phaseMs(Phase.COMPONENT_ENUMERATION);
  }

  public int componentCount() {
    return phaseCount(Phase.COMPONENT_ENUMERATION);
  }

  public long partitionGenerationMs() {
    return phaseMs(Phase.PARTITION_GENERATION);
  }

  public int partitionsExplored() {
    return phaseCount(Phase.PARTITION_GENERATION);
  }

  public long expressionBuildingMs() {
    return phaseMs(Phase.EXPRESSION_BUILDING);
  }

  public long deduplicationMs() {
    return phaseMs(Phase.DEDUPLICATION);
  }

  public int uniqueDecompositions() {
    return phaseCount(Phase.DEDUPLICATION);
  }

  public long totalMs() {
    return phaseMs(Phase.TOTAL);
  }

  /** Computed overhead (time not accounted for in decomposition phases). */
  public long overheadMs() {
    return totalMs()
        - componentEnumerationMs()
        - partitionGenerationMs()
        - expressionBuildingMs()
        - deduplicationMs();
  }

  /** Returns a percentage breakdown string. */
  public String percentageBreakdown() {
    if (totalMs() == 0) {
      return "No timing data";
    }
    return String.format(
        """
                        Phase breakdown (%%):
                          Component enumeration: %.1f%%
                          Partition generation:  %.1f%%
                          Expression building:   %.1f%%
                          Deduplication:         %.1f%%
                          Overhead:              %.1f%%""",
        100.0 * componentEnumerationMs() / totalMs(),
        100.0 * partitionGenerationMs() / totalMs(),
        100.0 * expressionBuildingMs() / totalMs(),
        100.0 * deduplicationMs() / totalMs(),
        100.0 * overheadMs() / totalMs());
  }

  @Override
  public String toString() {
    return String.format(
        """
                        === Run Metrics ===
                        Component enumeration: %d ms (%d components)
                        Partition generation:  %d ms (%d partitions)
                        Expression building:   %d ms
                        Deduplication:         %d ms (%d unique)
                        Overhead:              %d ms
                        ---------------------------
                        TOTAL:                 %d ms""",
        componentEnumerationMs(),
        componentCount(),
        partitionGenerationMs(),
        partitionsExplored(),
        expressionBuildingMs(),
        deduplicationMs(),
        uniqueDecompositions(),
        overheadMs(),
        totalMs());
  }

  private long phaseMs(Phase phase) {
    return phaseMs[phase.ordinal()];
  }

  private int phaseCount(Phase phase) {
    return phaseCounts[phase.ordinal()];
  }
}
