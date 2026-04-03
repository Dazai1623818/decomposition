package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.bench.BenchTypes.ComparisonPreparationStatus;
import evaluator.bench.BenchTypes.ExplorePreparationStatus;
import evaluator.index.CpqIndex;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchEngineWorkflowTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";
    private static final String COMPLEX_QUERY = "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,x1)";
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
                BenchTypes.EvaluationMode.ROWS,
                0,
                0,
                1,
                0,
                123L,
                compareLog,
                decompositionLog,
                false);
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
        assertTrue(compareContent.contains("--rows"));
        assertTrue(compareContent.contains("summary query_count=1"));
        assertTrue(Files.exists(decompositionLog));
    }

    @Test
    void compareFileCommandReflectsRequestedEvaluationMode(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "compare-mode.txt", VALID_QUERY);
        Path compareLog = tempDir.resolve("compare-mode.log");

        BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                Path.of("fake.idx"),
                queriesFile,
                BenchTypes.EvaluationMode.COUNT,
                0,
                0,
                1,
                0,
                123L,
                compareLog,
                null,
                false);
        engine.compareFile(spec);

        String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
        assertTrue(compareContent.contains("--count"));
        assertTrue(compareContent.contains("summary query_count=1"));
    }

    @Test
    void compareFileMarksParseFailureAsError(@TempDir Path tempDir) throws Exception {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        Path queriesFile = writeQueries(tempDir, "invalid-queries.txt", INVALID_QUERY);
        Path compareLog = tempDir.resolve("invalid-compare.log");

        BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                Path.of("fake.idx"),
                queriesFile,
                BenchTypes.EvaluationMode.ROWS,
                0,
                0,
                1,
                0,
                123L,
                compareLog,
                null,
                false);
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

    @Test
    void compareFileTimesEachMethodInIsolation(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge,cost");
        try {
            BenchEngine engine = new BenchEngine(new SlowSupportsIndex(80L));
            Path queriesFile = writeQueries(tempDir, "isolated-compare.txt", COMPLEX_QUERY);
            Path compareLog = tempDir.resolve("isolated-compare.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    40,
                    0,
                    1,
                    0,
                    123L,
                    compareLog,
                    null,
                    false);
            BenchTypes.CompareFileReport report = engine.compareFile(spec);

            assertEquals(1L, report.okRows());
            assertEquals(0L, report.timeoutRows());
            assertEquals(1L, report.decompositionTimeoutRows());

            List<String> lines = Files.readAllLines(compareLog, StandardCharsets.UTF_8);
            String singleEdge = findMethodLine(lines, "SINGLE_EDGE");
            String cost = findMethodLine(lines, "COST");
            assertTrue(singleEdge.contains("status=OK"), singleEdge);
            assertTrue(cost.contains("status=PLANNING_TIMEOUT"), cost);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareFileReportsExecutionTimeoutSeparately(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new SlowQueryIndex(80L));
            Path queriesFile = writeQueries(tempDir, "exec-timeout-compare.txt", VALID_QUERY);
            Path compareLog = tempDir.resolve("exec-timeout-compare.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    40,
                    0,
                    1,
                    0,
                    123L,
                    compareLog,
                    null,
                    false);
            BenchTypes.CompareFileReport report = engine.compareFile(spec);

            assertEquals(0L, report.okRows());
            assertEquals(1L, report.timeoutRows());
            assertEquals(0L, report.decompositionTimeoutRows());

            String row = findMethodLine(Files.readAllLines(compareLog, StandardCharsets.UTF_8), "SINGLE_EDGE");
            assertTrue(row.contains("status=EXEC_TIMEOUT"), row);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareFilePreservesUnboundedCoverLimit(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "unbounded-cover.txt", VALID_QUERY);
            Path compareLog = tempDir.resolve("unbounded-cover.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    0,
                    0,
                    0,
                    0,
                    123L,
                    compareLog,
                    null,
                    false);
            engine.compareFile(spec);

            String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
            assertTrue(compareContent.contains("cover_limit=0"));
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareFileSharesParseTimeAndReportsEndToEndTiming(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge,cost");
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "timing-compare.txt", VALID_QUERY);
            Path compareLog = tempDir.resolve("timing-compare.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    0,
                    0,
                    1,
                    0,
                    123L,
                    compareLog,
                    null,
                    false);
            engine.compareFile(spec);

            List<String> lines = Files.readAllLines(compareLog, StandardCharsets.UTF_8);
            String singleEdge = findMethodLine(lines, "SINGLE_EDGE");
            String cost = findMethodLine(lines, "COST");

            double singleParse = parseDoubleField(singleEdge, "parse_ms");
            double costParse = parseDoubleField(cost, "parse_ms");
            assertEquals(singleParse, costParse, 0.0);

            double singleWall = parseDoubleField(singleEdge, "method_wall_ms");
            double singleEndToEnd = parseDoubleField(singleEdge, "end_to_end_ms");
            assertEquals(singleParse + singleWall, singleEndToEnd, 5.0);
            assertTrue(singleEdge.contains("end_to_end_ms="), singleEdge);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareFileReportsExecutionBreakdownThatAddsUp(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "compare-breakdown.txt", VALID_QUERY);
            Path compareLog = tempDir.resolve("compare-breakdown.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    0,
                    0,
                    1,
                    0,
                    123L,
                    compareLog,
                    null,
                    false);
            engine.compareFile(spec);

            String row = findMethodStatusLine(Files.readAllLines(compareLog, StandardCharsets.UTF_8), "SINGLE_EDGE", "OK");
            double execution = parseDoubleField(row, "execution_ms");
            double indexLookup = parseDoubleField(row, "index_lookup_ms");
            double mapping = parseDoubleField(row, "mapping_ms");
            double joinOrder = parseDoubleField(row, "join_order_ms");
            double join = parseDoubleField(row, "join_ms");
            assertEquals(indexLookup + mapping + joinOrder + join, execution, 0.05, row);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void estimationBenchReportsExecutionBreakdownThatAddsUp(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "estimation-breakdown.txt", VALID_QUERY);
            Path out = tempDir.resolve("estimation-breakdown.log");

            BenchTypes.EstimationBenchSpec spec = new BenchTypes.EstimationBenchSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    null,
                    0,
                    0,
                    1,
                    0,
                    123L,
                    out);
            engine.estimationBench(spec);

            String row = findMethodStatusLine(Files.readAllLines(out, StandardCharsets.UTF_8), "SINGLE_EDGE", "OK");
            double execution = parseDoubleField(row, "execution_ms");
            double indexLookup = parseDoubleField(row, "index_lookup_ms");
            double mapping = parseDoubleField(row, "mapping_ms");
            double joinOrder = parseDoubleField(row, "join_order_ms");
            double join = parseDoubleField(row, "join_ms");
            assertEquals(indexLookup + mapping + joinOrder + join, execution, 0.05, row);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareFileDoesNotCloseStdout(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        PrintStream previousOut = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setProperty("cpq.decompose.methods", "single_edge");
        System.setOut(new PrintStream(capture, true, StandardCharsets.UTF_8));
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "stdout-compare.txt", VALID_QUERY);

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    0,
                    0,
                    1,
                    0,
                    123L,
                    null,
                    null,
                    false);
            engine.compareFile(spec);
            System.out.println("after-compare");

            String output = capture.toString(StandardCharsets.UTF_8);
            assertTrue(output.contains("after-compare"), output);
        } finally {
            System.setOut(previousOut);
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void exploreReportsPreparationStatus() {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());
        BenchTypes.ExploreSpec spec = new BenchTypes.ExploreSpec(
                Path.of("fake.idx"),
                VALID_QUERY,
                1,
                0,
                0,
                0,
                2,
                0,
                0,
                0,
                0,
                123L);

        BenchTypes.ExploreReport report = engine.explore(spec);

        assertEquals(ExplorePreparationStatus.NO_DECOMPOSITIONS_AFTER_COMPONENT_FILTER, report.status());
        assertEquals(0, report.candidateCount());
    }

    @Test
    void compareFileWarmupLogsSeparateWarmupSummary(@TempDir Path tempDir) throws Exception {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new FakeCpqIndex());
            Path queriesFile = writeQueries(tempDir, "warmup-compare.txt", VALID_QUERY);
            Path compareLog = tempDir.resolve("warmup-compare.log");

            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    Path.of("fake.idx"),
                    queriesFile,
                    BenchTypes.EvaluationMode.ROWS,
                    0,
                    0,
                    1,
                    0,
                    123L,
                    compareLog,
                    null,
                    true);
            BenchTypes.CompareFileReport report = engine.compareFile(spec);

            assertEquals(1, report.queryCount());

            String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
            assertTrue(compareContent.contains("warmup_enabled=true"));
            assertTrue(compareContent.contains("warmup_query_count="));
            assertTrue(compareContent.contains("warmup query_count="));
            assertTrue(compareContent.contains("summary query_count=1"));
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareReportsNoDecompositionsStatus() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge");
        try {
            BenchEngine engine = new BenchEngine(new UnsupportedCpqIndex());
            BenchTypes.CompareSpec spec = new BenchTypes.CompareSpec(
                    Path.of("fake.idx"),
                    VALID_QUERY,
                    BenchTypes.EvaluationMode.COUNT,
                    1,
                    0,
                    0,
                    0,
                    123L);

            BenchTypes.CompareReport report = engine.compare(spec);

            assertEquals(ComparisonPreparationStatus.NO_DECOMPOSITIONS, report.status());
            assertEquals(0, report.comparedMethods());
            assertEquals(0, report.timeoutCount());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void compareCountsPlanningTimeouts() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "cost");
        try {
            BenchEngine engine = new BenchEngine(new SlowSupportsIndex(80L));
            BenchTypes.CompareSpec spec = new BenchTypes.CompareSpec(
                    Path.of("fake.idx"),
                    COMPLEX_QUERY,
                    BenchTypes.EvaluationMode.COUNT,
                    1,
                    2,
                    40,
                    40,
                    123L);

            BenchTypes.CompareReport report = engine.compare(spec);

            assertEquals(ComparisonPreparationStatus.READY, report.status());
            assertEquals(0, report.comparedMethods());
            assertEquals(1, report.timeoutCount());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    private static Path writeQueries(Path dir, String fileName, String... queries) throws Exception {
        Path file = dir.resolve(fileName);
        Files.write(file, List.of(queries), StandardCharsets.UTF_8);
        return file;
    }

    private static String findMethodLine(List<String> lines, String methodName) {
        String token = "method=" + methodName;
        for (String line : lines) {
            if (line.contains(token)) {
                return line;
            }
        }
        throw new AssertionError(String.format(Locale.ROOT, "Missing line for %s", methodName));
    }

    private static String findMethodStatusLine(List<String> lines, String methodName, String status) {
        String methodToken = "method=" + methodName;
        String statusToken = "status=" + status;
        for (String line : lines) {
            if (line.contains(methodToken) && line.contains(statusToken)) {
                return line;
            }
        }
        throw new AssertionError(String.format(
                Locale.ROOT,
                "Missing line for method=%s status=%s",
                methodName,
                status));
    }

    private static double parseDoubleField(String line, String field) {
        String token = field + "=";
        for (String part : line.split(" ")) {
            if (part.startsWith(token)) {
                return Double.parseDouble(part.substring(token.length()));
            }
        }
        throw new AssertionError(String.format(Locale.ROOT, "Missing field %s in %s", field, line));
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static class FakeCpqIndex implements CpqIndex {
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

    private static final class SlowQueryIndex extends FakeCpqIndex {
        private final long queryDelayMs;

        private SlowQueryIndex(long queryDelayMs) {
            this.queryDelayMs = queryDelayMs;
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            try {
                Thread.sleep(queryDelayMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while querying", ex);
            }
            return super.query(cpq);
        }
    }

    private static final class SlowSupportsIndex extends FakeCpqIndex {
        private final long supportsDelayMs;

        private SlowSupportsIndex(long supportsDelayMs) {
            this.supportsDelayMs = supportsDelayMs;
        }

        @Override
        public boolean supports(CPQ cpq) {
            if (isAtomic(cpq)) {
                return true;
            }
            try {
                Thread.sleep(supportsDelayMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while checking support", ex);
            }
            return true;
        }

        private static boolean isAtomic(CPQ cpq) {
            String text = cpq.toString();
            return !text.contains("◦") && !text.contains("∩");
        }
    }

    private static final class UnsupportedCpqIndex extends FakeCpqIndex {
        @Override
        public boolean isIndexable(CPQ cpq) {
            return false;
        }
    }
}
