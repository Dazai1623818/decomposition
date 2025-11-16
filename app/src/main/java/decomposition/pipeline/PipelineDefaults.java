package decomposition.pipeline;

/** Shared constants for the decomposition pipeline services. */
public final class PipelineDefaults {
  private PipelineDefaults() {}

  /** Maximum allowed join nodes per component during partitioning/filtering. */
  public static final int MAX_JOIN_NODES = 2;
}
