package decomposition.testing;

/**
 * Centralized test configuration knobs. Allows Gradle/JVM runners to control expensive
 * functionality (like tuple enumeration) via environment variables or system properties.
 */
public final class TestDefaults {
  private static final String SINGLE_TUPLE_PROPERTY = "decomposition.singleTuplePerPartition";
  private static final String SINGLE_TUPLE_ENV = "DECOMPOSITION_SINGLE_TUPLE";
  private static final boolean DEFAULT_SINGLE_TUPLE = true;

  private TestDefaults() {}

  /**
   * Returns whether tests should restrict enumeration to a single tuple per partition. Defaults to
   * {@code true} (to keep test runs fast) but can be overridden via either the system property
   * {@code decomposition.singleTuplePerPartition} or the environment variable {@code
   * DECOMPOSITION_SINGLE_TUPLE}.
   */
  public static boolean singleTuplePerPartition() {
    String propertyValue = System.getProperty(SINGLE_TUPLE_PROPERTY);
    if (propertyValue != null) {
      return Boolean.parseBoolean(propertyValue);
    }
    String envValue = System.getenv(SINGLE_TUPLE_ENV);
    if (envValue != null) {
      return Boolean.parseBoolean(envValue);
    }
    return DEFAULT_SINGLE_TUPLE;
  }
}
