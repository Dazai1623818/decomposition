package evaluator.bench;

import java.util.Locale;

/**
 * Runtime configuration for CPQ decomposition, planning, and estimation.
 */
public record EngineConfig(
        boolean joinSafeDistinctFastPath,
        boolean estimateHeuristicJoinOrders,
        long estimationSeed,
        int systemRMaxCandidateOrders) {
    private static final long DEFAULT_ESTIMATION_SEED = 0xC0FFEE;
    private static final int DEFAULT_SYSTEM_R_MAX_CANDIDATE_ORDERS = 0;

    /**
     * Builds default configuration without reading system properties.
     */
    public static EngineConfig defaults() {
        return new EngineConfig(
                true,
                false,
                DEFAULT_ESTIMATION_SEED,
                DEFAULT_SYSTEM_R_MAX_CANDIDATE_ORDERS);
    }

    /**
     * Builds configuration from supported system properties with defaults that
     * match current evaluator behavior.
     */
    public static EngineConfig fromSystemProperties() {
        return new EngineConfig(
                booleanProperty("cpq.join.safeDistinctFastPath", true),
                booleanProperty("cpq.join.estimateHeuristicPlans", false),
                longProperty("cpq.estimate.seed", DEFAULT_ESTIMATION_SEED),
                intProperty(
                        "cpq.systemr.maxCandidateOrders",
                        DEFAULT_SYSTEM_R_MAX_CANDIDATE_ORDERS));
    }

    private static boolean booleanProperty(String name, boolean fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "true", "1", "yes", "on" -> true;
            case "false", "0", "no", "off" -> false;
            default -> fallback;
        };
    }

    private static long longProperty(String name, long fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static int intProperty(String name, int fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    public EngineConfig withEstimationSeed(long seed) {
        return new EngineConfig(
                joinSafeDistinctFastPath,
                estimateHeuristicJoinOrders,
                seed,
                systemRMaxCandidateOrders);
    }

    public EngineConfig withSystemRMaxCandidateOrders(int maxCandidateOrders) {
        return new EngineConfig(
                joinSafeDistinctFastPath,
                estimateHeuristicJoinOrders,
                estimationSeed,
                Math.max(0, maxCandidateOrders));
    }
}
