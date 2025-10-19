package decomposition;

/**
 * Configuration for the CQ to CPQ decomposition pipeline.
 */
public record DecompositionOptions(
        Mode mode,
        int maxPartitions,
        int maxCovers,
        long timeBudgetMs) {

    public static DecompositionOptions defaults() {
        return new DecompositionOptions(Mode.DECOMPOSE, 10_000, 500, 5_000);
    }

    public enum Mode {
        PARTITIONS,
        DECOMPOSE,
        BOTH;

        public boolean partitionsEnabled() {
            return this == PARTITIONS || this == BOTH;
        }

        public boolean decomposeEnabled() {
            return this == DECOMPOSE || this == BOTH;
        }
    }
}
