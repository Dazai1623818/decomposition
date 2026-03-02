package evaluator.bench;

import java.util.Locale;
import java.util.Objects;

/**
 * Runtime configuration for CPQ decomposition, planning, and estimation.
 */
public record EngineConfig(
        boolean defaultDecomposeUseBest,
        int defaultDecomposeCoverLimit,
        int defaultDecomposeSelectionWalks,
        int defaultDecomposeSelectionRandomOrders,
        int orderEstimationWalks,
        int orderEstimationRandomOrders,
        EstimatorType estimatorType,
        boolean wanderJoinRequireExtension,
        boolean joinSafeDistinctFastPath,
        long estimationSeed) {
    private static final long DEFAULT_ESTIMATION_SEED = 0xC0FFEE;

    public EngineConfig {
        requirePositive(defaultDecomposeCoverLimit, "defaultDecomposeCoverLimit");
        requireNonNegative(defaultDecomposeSelectionWalks, "defaultDecomposeSelectionWalks");
        requireNonNegative(defaultDecomposeSelectionRandomOrders, "defaultDecomposeSelectionRandomOrders");
        requireNonNegative(orderEstimationWalks, "orderEstimationWalks");
        requireNonNegative(orderEstimationRandomOrders, "orderEstimationRandomOrders");
        Objects.requireNonNull(estimatorType, "estimatorType");
    }

    /**
     * Builds default configuration without reading system properties.
     */
    public static EngineConfig defaults() {
        return new EngineConfig(
                true,
                1,
                0,
                0,
                0,
                0,
                EstimatorType.WANDERJOIN,
                true,
                true,
                DEFAULT_ESTIMATION_SEED);
    }

    /**
     * Builds configuration from supported system properties with defaults that
     * match current evaluator behavior.
     */
    public static EngineConfig fromSystemProperties() {
        return new EngineConfig(
                booleanProperty("cpq.decompose.useBest", true),
                positiveIntProperty("cpq.decompose.coverLimit", 1),
                nonNegativeIntProperty("cpq.decompose.selection.walks", 0),
                nonNegativeIntProperty("cpq.decompose.selection.randomOrders", 0),
                nonNegativeIntProperty("cpq.order.estimate.walks", 0),
                nonNegativeIntProperty("cpq.order.estimate.randomOrders", 0),
                EstimatorType.property("cpq.estimate.type", EstimatorType.WANDERJOIN),
                booleanProperty("cpq.estimate.wj.requireExtension", true),
                booleanProperty("cpq.join.safeDistinctFastPath", true),
                longProperty("cpq.estimate.seed", DEFAULT_ESTIMATION_SEED));
    }

    private static void requirePositive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be >= 1");
        }
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static int positiveIntProperty(String name, int fallback) {
        Integer value = Integer.getInteger(name);
        if (value == null || value < 1) {
            return fallback;
        }
        return value;
    }

    private static int nonNegativeIntProperty(String name, int fallback) {
        Integer value = Integer.getInteger(name);
        if (value == null || value < 0) {
            return fallback;
        }
        return value;
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

    public enum EstimatorType {
        WANDERJOIN,
        SYSTEM_R;

        static EstimatorType property(String name, EstimatorType fallback) {
            String raw = System.getProperty(name);
            if (raw == null || raw.isBlank()) {
                return fallback;
            }
            String normalized = raw.trim();
            for (EstimatorType type : values()) {
                if (type.name().equalsIgnoreCase(normalized)) {
                    return type;
                }
            }
            return fallback;
        }
    }

    /**
     * Returns a copy of this configuration with a different estimation seed.
     */
    public EngineConfig withEstimationSeed(long seed) {
        return new EngineConfig(
                defaultDecomposeUseBest,
                defaultDecomposeCoverLimit,
                defaultDecomposeSelectionWalks,
                defaultDecomposeSelectionRandomOrders,
                orderEstimationWalks,
                orderEstimationRandomOrders,
                estimatorType,
                wanderJoinRequireExtension,
                joinSafeDistinctFastPath,
                seed);
    }

}
