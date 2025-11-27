package decomposition.eval.engine;

import java.util.Map;
import java.util.Objects;

/** Holds the raw forward and reverse adjacency lists for a relation. */
public final class RelationProjection {
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private final int[] allSources;
  private final int[] allTargets;
  private final Map<Integer, int[]> forward;
  private final Map<Integer, int[]> reverse;

  public RelationProjection(
      int[] allSources,
      int[] allTargets,
      Map<Integer, int[]> forward,
      Map<Integer, int[]> reverse) {
    this.allSources = Objects.requireNonNull(allSources, "allSources");
    this.allTargets = Objects.requireNonNull(allTargets, "allTargets");
    this.forward = Objects.requireNonNull(forward, "forward");
    this.reverse = Objects.requireNonNull(reverse, "reverse");
  }

  public boolean isEmpty() {
    return allSources.length == 0 || allTargets.length == 0;
  }

  public int[] allSources() {
    return allSources;
  }

  public int[] allTargets() {
    return allTargets;
  }

  public int[] targetsForSource(int source) {
    return forward.getOrDefault(source, EMPTY_INT_ARRAY);
  }

  public int[] sourcesForTarget(int target) {
    return reverse.getOrDefault(target, EMPTY_INT_ARRAY);
  }
}
