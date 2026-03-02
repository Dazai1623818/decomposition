package evaluator.evaluation;

/**
 * Supported decomposition strategies used by planner and benchmark reporting.
 */
public enum DecompositionMethod {
    SINGLE_EDGE("single_edge"),
    COST("cost"),
    DIAMETER("diameter"),
    TW2_COST_DP("tw2_cost_dp"),
    SERIES_PARALLEL("series_parallel");

    private final String id;

    DecompositionMethod(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }
}
