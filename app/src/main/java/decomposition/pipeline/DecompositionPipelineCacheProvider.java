package decomposition.pipeline;

import decomposition.cpq.model.CacheStats;

/** Provides access to the pipeline cache so default implementations can expose summary data. */
public interface DecompositionPipelineCacheProvider {
  DecompositionPipelineCache pipelineCache();

  default CacheStats lastCacheSnapshot() {
    DecompositionPipelineCache cache = pipelineCache();
    return (cache != null) ? cache.lastSnapshot() : null;
  }
}
