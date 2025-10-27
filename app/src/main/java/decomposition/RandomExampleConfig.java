package decomposition;

import java.util.Random;

/**
 * Configuration parameters for the random CQ example to control the overall size and structure of
 * the generated query.
 */
public record RandomExampleConfig(
    int freeVariableCount, int edgeCount, int predicateLabelCount, Long seed) {

  public static final int DEFAULT_FREE_VARIABLES = 1;
  public static final int DEFAULT_EDGE_COUNT = 5;
  public static final int DEFAULT_PREDICATE_LABELS = 5;

  public RandomExampleConfig {
    if (freeVariableCount < 1) {
      throw new IllegalArgumentException("freeVariableCount must be at least 1");
    }
    if (edgeCount < 1) {
      throw new IllegalArgumentException("edgeCount must be at least 1");
    }
    if (predicateLabelCount < 1) {
      throw new IllegalArgumentException("predicateLabelCount must be at least 1");
    }
  }

  public static RandomExampleConfig defaults() {
    return new RandomExampleConfig(
        DEFAULT_FREE_VARIABLES, DEFAULT_EDGE_COUNT, DEFAULT_PREDICATE_LABELS, null);
  }

  public Random createRandom() {
    return seed != null ? new Random(seed) : new Random();
  }
}
