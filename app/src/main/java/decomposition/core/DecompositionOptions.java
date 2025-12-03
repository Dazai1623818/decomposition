package decomposition.core;

import decomposition.pipeline.PlanMode;

/** Configuration for the CQ to CPQ decomposition pipeline. */
public record DecompositionOptions(
    Mode mode,
    int maxPartitions,
    long timeBudgetMs,
    int tupleLimit,
    boolean deepVerification,
    PlanMode planMode) {

  public static DecompositionOptions defaults() {
    return new DecompositionOptions(Mode.VALIDATE, Integer.MAX_VALUE, 0, 0, false, PlanMode.ALL);
  }

  public enum Mode {
    VALIDATE,
    ENUMERATE;

    public boolean enumerateTuples() {
      return this == ENUMERATE;
    }
  }
}
