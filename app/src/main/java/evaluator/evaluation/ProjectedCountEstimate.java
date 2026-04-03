package evaluator.evaluation;

/**
 * Shared estimate shape used by deterministic projected-cardinality scoring.
 */
public record ProjectedCountEstimate(double estimatedCount) {
    /**
     * Per-prefix projected-cardinality estimate for variable-order prefixes.
     */
    public record PrefixEstimate(double estimatedCount, long estimateNanos) {
    }
}
