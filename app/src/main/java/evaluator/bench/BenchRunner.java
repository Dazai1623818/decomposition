package evaluator.bench;

import evaluator.index.CpqIndex;
import java.util.Objects;

/**
 * Public benchmark orchestrator facade. Internal execution logic is delegated
 * to package-private {@link BenchEngine} to keep this API surface small.
 */
public final class BenchRunner {
    private final BenchEngine delegate;

    public BenchRunner(CpqIndex index) {
        this(index, EngineConfig.defaults(), null);
    }

    public BenchRunner(CpqIndex index, EngineConfig config) {
        this(index, config, null);
    }

    public BenchRunner(CpqIndex index, EngineConfig config, MemoryDiagnostics.IndexLoadStats indexLoadStats) {
        this.delegate = new BenchEngine(
                Objects.requireNonNull(index, "index"),
                Objects.requireNonNull(config, "config"),
                indexLoadStats);
    }

    public BenchTypes.EvalFileReport evalFile(BenchTypes.EvalFileSpec spec) {
        return delegate.evalFile(spec);
    }

    public BenchTypes.ExploreReport explore(BenchTypes.ExploreSpec spec) {
        return delegate.explore(spec);
    }

    public BenchTypes.CompareReport compare(BenchTypes.CompareSpec spec) {
        return delegate.compare(spec);
    }

    public BenchTypes.ProfileReport profile(BenchTypes.ProfileSpec spec) {
        return delegate.profile(spec);
    }

    public BenchTypes.EstimateReport estimate(BenchTypes.EstimateSpec spec) {
        return delegate.estimate(spec);
    }

    public BenchTypes.CompareFileReport compareFile(BenchTypes.CompareFileSpec spec) throws Exception {
        return delegate.compareFile(spec);
    }

    public BenchTypes.EstimationBenchReport estimationBench(BenchTypes.EstimationBenchSpec spec) throws Exception {
        return delegate.estimationBench(spec);
    }
}
