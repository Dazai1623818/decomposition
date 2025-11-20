package decomposition.generator;

/** Shared constants for CPQ builder generation steps. */
public final class GeneratorDefaults {
  private GeneratorDefaults() {}

  /** Maximum allowed join nodes per component during partitioning/filtering. */
  public static final int MAX_JOIN_NODES = 2;
}
