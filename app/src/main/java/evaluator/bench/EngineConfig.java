package evaluator.bench;

import java.util.Locale;

/**
 * Runtime configuration for CPQ decomposition, planning, and estimation.
 */
public record EngineConfig(
        boolean defaultDecomposeUseBest,
        int defaultDecomposeCoverLimit,
        int defaultDecomposeSelectionWalks,
        int defaultDecomposeSelectionRandomOrders,
        int defaultDecomposeSelectionStructuredOrders,
        int decompositionEstimationTopK,
        int decompositionEstimationMinComponents,
        int decompositionEstimationStage1Walks,
        int decompositionEstimationStage1RandomOrders,
        int decompositionEstimationStage1StructuredOrders,
        int decompositionEstimationStage2TopK,
        int decompositionEstimationStage2Walks,
        int decompositionEstimationStage2RandomOrders,
        int decompositionEstimationStage2StructuredOrders,
        int orderEstimationWalks,
        int orderEstimationRandomOrders,
        int orderEstimationStructuredOrders,
        int orderEstimationHardMinComponents,
        int orderEstimationHardWalks,
        int orderEstimationHardRandomOrders,
        int orderEstimationHardStructuredOrders,
        boolean orderEstimationProjectedPrefixCandidate,
        boolean orderEstimationSingleProjectedHubFirst,
        int orderEstimationSingleProjectedHubMinDegree,
        double orderEstimationSingleProjectedHubPenalty,
        boolean joinSafeDistinctFastPath,
        double estimationZ,
        long estimationSeed) {

    public EngineConfig {
        requirePositive(defaultDecomposeCoverLimit, "defaultDecomposeCoverLimit");
        requirePositive(defaultDecomposeSelectionWalks, "defaultDecomposeSelectionWalks");
        requireNonNegative(defaultDecomposeSelectionRandomOrders, "defaultDecomposeSelectionRandomOrders");
        requireNonNegative(defaultDecomposeSelectionStructuredOrders, "defaultDecomposeSelectionStructuredOrders");
        requirePositive(decompositionEstimationTopK, "decompositionEstimationTopK");
        requirePositive(decompositionEstimationMinComponents, "decompositionEstimationMinComponents");
        requirePositive(decompositionEstimationStage1Walks, "decompositionEstimationStage1Walks");
        requireNonNegative(decompositionEstimationStage1RandomOrders, "decompositionEstimationStage1RandomOrders");
        requireNonNegative(decompositionEstimationStage1StructuredOrders, "decompositionEstimationStage1StructuredOrders");
        requirePositive(decompositionEstimationStage2TopK, "decompositionEstimationStage2TopK");
        requirePositive(decompositionEstimationStage2Walks, "decompositionEstimationStage2Walks");
        requireNonNegative(decompositionEstimationStage2RandomOrders, "decompositionEstimationStage2RandomOrders");
        requireNonNegative(decompositionEstimationStage2StructuredOrders, "decompositionEstimationStage2StructuredOrders");
        requirePositive(orderEstimationWalks, "orderEstimationWalks");
        requireNonNegative(orderEstimationRandomOrders, "orderEstimationRandomOrders");
        requireNonNegative(orderEstimationStructuredOrders, "orderEstimationStructuredOrders");
        requirePositive(orderEstimationHardMinComponents, "orderEstimationHardMinComponents");
        requirePositive(orderEstimationHardWalks, "orderEstimationHardWalks");
        requireNonNegative(orderEstimationHardRandomOrders, "orderEstimationHardRandomOrders");
        requireNonNegative(orderEstimationHardStructuredOrders, "orderEstimationHardStructuredOrders");
        requirePositive(orderEstimationSingleProjectedHubMinDegree, "orderEstimationSingleProjectedHubMinDegree");
        requirePositive(orderEstimationSingleProjectedHubPenalty, "orderEstimationSingleProjectedHubPenalty");
        requirePositive(estimationZ, "estimationZ");
    }

    /**
     * Builds default configuration without reading system properties.
     */
    public static EngineConfig defaults() {
        return new EngineConfig(
                true,
                1,
                8,
                1,
                1,
                4,
                1,
                8,
                1,
                1,
                2,
                64,
                3,
                2,
                32,
                4,
                0,
                4,
                32,
                4,
                0,
                true,
                true,
                2,
                1.5d,
                true,
                1.96d,
                0xC0FFEE);
    }

    /**
     * Builds configuration from supported system properties with defaults that
     * match current evaluator behavior.
     */
    public static EngineConfig fromSystemProperties() {
        int decompositionEstimationWalks = positiveIntProperty(
                "cpq.decomposition.estimate.walks",
                64);
        int decompositionEstimationStage1Walks = positiveIntProperty(
                "cpq.decomposition.estimate.stage1.walks",
                8);
        int orderEstimationWalks = positiveIntProperty(
                "cpq.order.estimate.walks",
                32);
        int orderEstimationRandomOrders = nonNegativeIntProperty(
                "cpq.order.estimate.randomOrders",
                4);
        int orderEstimationStructuredOrders = nonNegativeIntProperty(
                "cpq.order.estimate.structuredOrders",
                0);

        return new EngineConfig(
                booleanProperty("cpq.decompose.useBest", true),
                positiveIntProperty("cpq.decompose.coverLimit", 1),
                positiveIntProperty("cpq.decompose.selection.walks", 8),
                nonNegativeIntProperty("cpq.decompose.selection.randomOrders", 1),
                nonNegativeIntProperty("cpq.decompose.selection.structuredOrders", 1),
                positiveIntProperty("cpq.decomposition.estimate.topk", 4),
                positiveIntProperty("cpq.decomposition.estimate.minComponents", 1),
                decompositionEstimationStage1Walks,
                nonNegativeIntProperty("cpq.decomposition.estimate.stage1.randomOrders", 1),
                nonNegativeIntProperty("cpq.decomposition.estimate.stage1.structuredOrders", 1),
                positiveIntProperty("cpq.decomposition.estimate.stage2.topk", 2),
                positiveIntProperty("cpq.decomposition.estimate.stage2.walks", decompositionEstimationWalks),
                nonNegativeIntProperty("cpq.decomposition.estimate.stage2.randomOrders", 3),
                nonNegativeIntProperty("cpq.decomposition.estimate.stage2.structuredOrders", 2),
                orderEstimationWalks,
                orderEstimationRandomOrders,
                orderEstimationStructuredOrders,
                positiveIntProperty("cpq.order.estimate.hard.minComponents", 4),
                positiveIntProperty("cpq.order.estimate.hard.walks", orderEstimationWalks),
                nonNegativeIntProperty("cpq.order.estimate.hard.randomOrders", orderEstimationRandomOrders),
                nonNegativeIntProperty("cpq.order.estimate.hard.structuredOrders", orderEstimationStructuredOrders),
                booleanProperty("cpq.order.estimate.projectedPrefixCandidate", true),
                booleanProperty("cpq.order.estimate.singleProjectedHubFirst", true),
                positiveIntProperty("cpq.order.estimate.singleProjectedHubMinDegree", 2),
                positiveDoubleProperty("cpq.order.estimate.singleProjectedHubPenalty", 1.5d),
                booleanProperty("cpq.join.safeDistinctFastPath", true),
                positiveDoubleProperty("cpq.estimate.z", 1.96d),
                longProperty("cpq.estimate.seed", 0xC0FFEE));
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

    private static void requirePositive(double value, String name) {
        if (!(value > 0.0d) || !Double.isFinite(value)) {
            throw new IllegalArgumentException(name + " must be finite and > 0");
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

    private static double positiveDoubleProperty(String name, double fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            double value = Double.parseDouble(raw);
            return value > 0.0 && Double.isFinite(value) ? value : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static long longProperty(String name, long fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
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
}
