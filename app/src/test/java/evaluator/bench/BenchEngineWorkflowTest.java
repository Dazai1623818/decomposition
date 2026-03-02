package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.index.CpqIndex;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchEngineWorkflowTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";
    private static final String INVALID_QUERY = "not_a_cq_query";

    @Test
    void compareFileTracksRowsForSuccessfulWorkload(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "queries.txt", VALID_QUERY);
        Path compareLog = tempDir.resolve("compare.log");
        Path decompositionLog = tempDir.resolve("decomposition.log");

        BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                Path.of("fake.idx"),
                queriesFile,
                0,
                0,
                1,
                0,
                123L,
                compareLog,
                decompositionLog);
        BenchTypes.CompareFileReport report = engine.compareFile(spec);

        assertEquals(1, report.queryCount());
        assertEquals(
                report.methodRows(),
                report.okRows()
                        + report.timeoutRows()
                        + report.decompositionTimeoutRows()
                        + report.noCandidateRows()
                        + report.errorRows());

        String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
        assertTrue(compareContent.contains("summary query_count=1"));
        assertTrue(Files.exists(decompositionLog));
    }

    @Test
    void compareFileMarksParseFailureAsError(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "invalid-queries.txt", INVALID_QUERY);
        Path compareLog = tempDir.resolve("invalid-compare.log");

        BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                Path.of("fake.idx"),
                queriesFile,
                0,
                0,
                1,
                0,
                123L,
                compareLog,
                null);
        BenchTypes.CompareFileReport report = engine.compareFile(spec);

        assertEquals(1, report.queryCount());
        assertEquals(report.methodRows(), report.errorRows());
        assertEquals(0L, report.okRows());
        assertEquals(0L, report.timeoutRows());
        assertEquals(0L, report.noCandidateRows());
        assertEquals(0L, report.decompositionTimeoutRows());

        String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
        assertTrue(compareContent.contains("status=ERROR"));
    }

    @Test
    void estimationBenchTracksRowsForSuccessfulWorkload(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "estimation-queries.txt", VALID_QUERY);
        Path out = tempDir.resolve("estimation.log");

        BenchTypes.EstimationBenchSpec spec = new BenchTypes.EstimationBenchSpec(
                Path.of("fake.idx"),
                queriesFile,
                null,
                0,
                0,
                1,
                0,
                8,
                123L,
                out);
        BenchTypes.EstimationBenchReport report = engine.estimationBench(spec);

        assertEquals(1, report.queryCount());
        assertEquals(
                report.methodRows(),
                report.okRows()
                        + report.timeoutRows()
                        + report.decompositionTimeoutRows()
                        + report.noCandidateRows()
                        + report.errorRows());
        assertTrue(report.stepRows() >= report.methodRows());

        String output = Files.readString(out, StandardCharsets.UTF_8);
        assertTrue(output.contains("# columns:"));
        assertTrue(output.contains("summary query_count=1"));
    }

    @Test
    void estimationBenchMarksParseFailureAsError(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "invalid-estimation-queries.txt", INVALID_QUERY);
        Path out = tempDir.resolve("invalid-estimation.log");

        BenchTypes.EstimationBenchSpec spec = new BenchTypes.EstimationBenchSpec(
                Path.of("fake.idx"),
                queriesFile,
                null,
                0,
                0,
                1,
                0,
                8,
                123L,
                out);
        BenchTypes.EstimationBenchReport report = engine.estimationBench(spec);

        assertEquals(1, report.queryCount());
        assertEquals(report.methodRows(), report.errorRows());
        assertEquals(report.methodRows(), report.stepRows());
        assertEquals(0L, report.okRows());
        assertEquals(0L, report.timeoutRows());
        assertEquals(0L, report.noCandidateRows());
        assertEquals(0L, report.decompositionTimeoutRows());

        String output = Files.readString(out, StandardCharsets.UTF_8);
        assertTrue(output.contains("status=ERROR"));
    }

    private static Path writeQueries(Path dir, String fileName, String... queries) throws Exception {
        Path file = dir.resolve(fileName);
        Files.write(file, List.of(queries), StandardCharsets.UTF_8);
        return file;
    }

    private static final class FakeCpqIndex implements CpqIndex {
        private final List<Edge> edges = List.of(
                new Edge(1, 2),
                new Edge(2, 3),
                new Edge(3, 4));

        @Override
        public int k() {
            return 2;
        }

        @Override
        public int intersections() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isIndexable(CPQ cpq) {
            return true;
        }

        @Override
        public long cost(CPQ cpq) {
            return 1L;
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return edges;
        }
    }
}
