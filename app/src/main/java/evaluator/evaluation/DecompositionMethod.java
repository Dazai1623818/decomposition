package evaluator.evaluation;

/**
 * Supported decomposition strategies used by planner and benchmark reporting.
 */
public enum DecompositionMethod {
    SINGLE_EDGE("single_edge"),
    COST("cost"),
    COST_OVERLAP("cost_overlap"),
    DIAMETER("diameter"),
    SERIES_PARALLEL("series_parallel"),
    SERIES_PARALLEL_OVERLAP("series_parallel_overlap");

    private final String id;

    DecompositionMethod(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }
}
