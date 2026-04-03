package evaluator.evaluation;

/**
 * Supported decomposition strategies used by planner and benchmark reporting.
 */
public enum DecompositionMethod {
    SINGLE_EDGE("single_edge"),
    COST("cost", "count_only"),
    MAX_COLLAPSE("max_collapse"),
    PATH_DECOMPOSITION("path_decomposition"),
    SERIES_PARALLEL("series_parallel"),
    SINGLE_EDGE_SYSTEM_R("single_edge_system_r"),
    COST_SYSTEM_R_ORDER_ONLY(
            "cost_system_r_order_only",
            "cost_order_estimate",
            "cost_local_order"),
    EXHAUSTIVE_SYSTEM_R("exhaustive_system_r"),
    EXHAUSTIVE_SYSTEM_R_COVER_ONLY("exhaustive_system_r_cover_only", "exhaustive_cover_estimate"),
    SERIES_PARALLEL_SYSTEM_R_ORDER_ONLY(
            "series_parallel_system_r_order_only",
            "series_parallel_order_estimate",
            "series_parallel_local_order"),
    SERIES_PARALLEL_SYSTEM_R("series_parallel_system_r"),
    SERIES_PARALLEL_SYSTEM_R_COVER_ONLY(
            "series_parallel_system_r_cover_only",
            "series_parallel_rerank_cover_only",
            "series_parallel_cover_estimate",
            "series_parallel_system_r_guided_only",
            "series_parallel_guided_estimate"),
    EXHAUSTIVE_LEAF_COST("exhaustive_leaf_cost", "terminal_leaf_cost", "leaf_cost", "series_parallel_leaf_cost"),
    EXHAUSTIVE_LEAF_SYSTEM_R(
            "exhaustive_leaf_system_r",
            "terminal_leaf_system_r",
            "leaf_system_r",
            "series_parallel_leaf_system_r");

    private final String id;
    private final String[] aliases;

    DecompositionMethod(String id, String... aliases) {
        this.id = id;
        this.aliases = aliases;
    }

    public String id() {
        return id;
    }

    public static DecompositionMethod fromToken(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Decomposition method token must not be blank");
        }
        for (DecompositionMethod method : values()) {
            if (method.matchesToken(token.trim())) {
                return method;
            }
        }
        throw new IllegalArgumentException("Unknown decomposition method: " + token);
    }

    boolean matchesToken(String token) {
        if (id.equalsIgnoreCase(token) || name().equalsIgnoreCase(token)) {
            return true;
        }
        for (String alias : aliases) {
            if (alias.equalsIgnoreCase(token)) {
                return true;
            }
        }
        return false;
    }
}
