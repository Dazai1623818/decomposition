package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.bench.BenchTypes.DecompositionCandidate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.ExecutablePlan;
import evaluator.index.NativeCpqIndex;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class IntermediateResultTracerTest {
    private static final Path INDEX_PATH = Path.of("index/robots.k2.idx");
    private static final String QUERY = "(x,y) ← 0(x,y)";

    @Test
    void tracePreservesCompletedStagesWhenDeadlineExpires() throws Exception {
        Assumptions.assumeTrue(Files.exists(INDEX_PATH), "Missing index file " + INDEX_PATH);

        NativeCpqIndex index = NativeCpqIndex.load(INDEX_PATH);
        BenchEngine engine = new BenchEngine(index);
        ConjunctiveQuery cq = engine.parseCQ(QUERY);
        DecompositionCandidate candidate = engine.selectBestCandidates(cq, 10, 2, 10_000).stream()
                .filter(current -> current.method() == DecompositionMethod.SINGLE_EDGE)
                .findFirst()
                .orElseThrow();
        ExecutablePlan executable = ExecutablePlan.compile(candidate.decomposition(), index, Long.MAX_VALUE);

        IntermediateResultTracer.Trace trace = IntermediateResultTracer.trace(
                executable,
                List.of("?x", "?y"),
                EngineConfig.defaults().joinSafeDistinctFastPath(),
                System.nanoTime() - 1L);

        assertEquals(IntermediateResultTracer.TraceStatus.EXEC_TIMEOUT, trace.status());
        assertEquals("1", trace.timeoutStageLabel());
        assertEquals("?x", trace.timeoutVariable());
        assertEquals(1, trace.stages().size());
        assertEquals("mat", trace.stages().get(0).stageLabel());
    }
}
