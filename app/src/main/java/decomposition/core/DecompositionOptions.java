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

  public static DecompositionOptions normalize(DecompositionOptions options) {
    if (options == null) {
      return defaults();
    }
    DecompositionOptions defaults = defaults();
    Mode mode = options.mode() != null ? options.mode() : defaults.mode();
    int maxPartitions = options.maxPartitions() > 0 ? options.maxPartitions() : defaults.maxPartitions();
    long timeBudgetMs = options.timeBudgetMs();
    int tupleLimit = Math.max(0, options.tupleLimit());
    boolean deepVerification = options.deepVerification();
    PlanMode planMode = options.planMode() != null ? options.planMode() : defaults.planMode();
    return new DecompositionOptions(
        mode, maxPartitions, timeBudgetMs, tupleLimit, deepVerification, planMode);
  }

  public enum Mode {
    VALIDATE,
    ENUMERATE;

    public boolean enumerateTuples() {
      return this == ENUMERATE;
    }
  }
}
