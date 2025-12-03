package decomposition.pipeline;

/** Selection of how many decompositions to return for a CQ. */
public enum PlanMode {
  /** Build exactly one partition where every edge is its own component. */
  SINGLE_EDGE,
  /** Return only the first valid decomposition encountered. */
  FIRST,
  /** Return a single valid decomposition using a randomized visit order. */
  RANDOM,
  /** Enumerate all valid decompositions (subject to max partition caps). */
  ALL
}
