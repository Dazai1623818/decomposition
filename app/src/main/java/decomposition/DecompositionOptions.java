package decomposition;

/** Configuration for the CQ to CPQ decomposition pipeline. */
public record DecompositionOptions(
    Mode mode,
    int maxPartitions,
    long timeBudgetMs,
    int enumerationLimit,
    boolean singleTuplePerPartition) {

  public static DecompositionOptions defaults() {
    return new DecompositionOptions(Mode.VALIDATE, 10_000, 0, 100, false);
  }

  public enum Mode {
    VALIDATE,
    ENUMERATE;

    public boolean enumerateTuples() {
      return this == ENUMERATE;
    }
  }
}
