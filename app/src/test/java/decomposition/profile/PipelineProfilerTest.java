package decomposition.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.DecompositionOptions;
import decomposition.cpq.model.CacheStats;
import decomposition.profile.PipelineProfiler.NamedQuery;
import decomposition.profile.PipelineProfiler.PipelineProfile;
import decomposition.profile.PipelineProfiler.ProfileRun;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PipelineProfilerTest {

  @Test
  void profilesDefaultExamples() {
    PipelineProfiler profiler = new PipelineProfiler();
    List<NamedQuery> queries = PipelineProfiler.defaultExamples();

    PipelineProfile profile = profiler.profile(queries, DecompositionOptions.defaults());
    assertEquals(queries.size(), profile.runs().size(), "Runs should match query count");

    for (ProfileRun run : profile.runs()) {
      CacheStats cache = run.cacheSnapshot();
      assertTrue(cache.lookups() >= 0, "Cache stats should never be negative");
    }

    CacheStats aggregate = profile.aggregateCache();
    assertTrue(aggregate.lookups() >= 0, "Aggregate cache lookups should be non-negative");
  }
}
