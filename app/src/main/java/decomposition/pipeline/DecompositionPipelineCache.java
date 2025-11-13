package decomposition.pipeline;

import decomposition.cpq.model.CacheStats;

/** Tracks the last snapshot of CPQ cache stats that the pipeline exposes. */
public final class DecompositionPipelineCache {
  private CacheStats lastSnapshot;

  public void reset() {
    lastSnapshot = null;
  }

  public void update(CacheStats cacheStats) {
    lastSnapshot = (cacheStats != null) ? cacheStats.snapshot() : null;
  }

  public CacheStats lastSnapshot() {
    return lastSnapshot;
  }
}
