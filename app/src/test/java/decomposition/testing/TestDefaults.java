package decomposition.testing;

/**
 * Centralized test configuration knobs. Allows Gradle/JVM runners to control expensive
 * functionality (like tuple enumeration) via environment variables or system properties.
 */
public final class TestDefaults {
  private static final String TUPLE_LIMIT_PROPERTY = "decomposition.tupleLimit";
  private static final String TUPLE_LIMIT_ENV = "DECOMPOSITION_TUPLE_LIMIT";
  private static final int DEFAULT_TUPLE_LIMIT = 1;

  private TestDefaults() {}

  /**
   * Returns the tuple enumeration cap for tests. Defaults to 1 (to keep runs fast) but can be
   * overridden via the system property {@code decomposition.tupleLimit} or environment variable
   * {@code DECOMPOSITION_TUPLE_LIMIT}.
   */
  public static int tupleLimit() {
    String propertyValue = System.getProperty(TUPLE_LIMIT_PROPERTY);
    if (propertyValue != null) {
      try {
        return Integer.parseInt(propertyValue);
      } catch (NumberFormatException ignored) {
        // fall back to env/default
      }
    }
    String envValue = System.getenv(TUPLE_LIMIT_ENV);
    if (envValue != null) {
      try {
        return Integer.parseInt(envValue);
      } catch (NumberFormatException ignored) {
        // fall through
      }
    }
    return DEFAULT_TUPLE_LIMIT;
  }
}
