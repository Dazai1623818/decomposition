package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.cpq.ConjunctiveQuery;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.Planner;
import evaluator.index.NativeCpqIndex;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class CoverLimitProbeTest {
    private static final String ENABLE_PROPERTY = "cpq.coverProbe.enabled";
    private static final String INDEX_PROPERTY = "cpq.coverProbe.index";
    private static final String QUERIES_PROPERTY = "cpq.coverProbe.queries";
    private static final String OUTPUT_PROPERTY = "cpq.coverProbe.output";
    private static final String K_PROPERTY = "cpq.coverProbe.k";
    private static final String COVER_LIMIT_PROPERTY = "cpq.coverProbe.coverLimit";
    private static final String DECOMPOSITION_TIMEOUT_PROPERTY = "cpq.coverProbe.decompositionTimeoutMs";

    private static final Path DEFAULT_INDEX = Path.of("local/indices/wikivote.idx");
    private static final Path DEFAULT_QUERIES = Path.of(
            "local/queries/topology-bench/workloads/full_non_zero/warmup/warmup.20_shapes.representatives.cq");
    private static final Path DEFAULT_OUTPUT = Path.of(
            "logs/cover_limit_probe_warmup_representatives_k10_limit99999.csv");
    private static final int DEFAULT_K = 10;
    private static final int DEFAULT_COVER_LIMIT = 99999;
    private static final int DEFAULT_DECOMPOSITION_TIMEOUT_MS = 0;

    @Test
    void logWarmupRepresentativeCoverCounts() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY), "Manual probe disabled");

        Path indexPath = Path.of(System.getProperty(INDEX_PROPERTY, DEFAULT_INDEX.toString()));
        Path queriesPath = Path.of(System.getProperty(QUERIES_PROPERTY, DEFAULT_QUERIES.toString()));
        Path outputPath = Path.of(System.getProperty(OUTPUT_PROPERTY, DEFAULT_OUTPUT.toString()));
        int k = Integer.getInteger(K_PROPERTY, DEFAULT_K);
        int coverLimit = Integer.getInteger(COVER_LIMIT_PROPERTY, DEFAULT_COVER_LIMIT);
        int decompositionTimeoutMs = Integer.getInteger(
                DECOMPOSITION_TIMEOUT_PROPERTY,
                DEFAULT_DECOMPOSITION_TIMEOUT_MS);

        Assumptions.assumeTrue(Files.exists(indexPath), "Missing index file " + indexPath);
        Assumptions.assumeTrue(Files.exists(queriesPath), "Missing queries file " + queriesPath);

        NativeCpqIndex index = NativeCpqIndex.load(indexPath);
        if (k > index.k()) {
            throw new IllegalArgumentException(
                    "Requested k=" + k + " exceeds index k=" + index.k() + " for " + indexPath);
        }

        Planner planner = new Planner(index);
        Files.createDirectories(outputPath.toAbsolutePath().getParent());

        System.out.println("cover_limit_probe_start");
        System.out.println("index=" + indexPath.toAbsolutePath());
        System.out.println("queries=" + queriesPath.toAbsolutePath());
        System.out.println("output=" + outputPath.toAbsolutePath());
        System.out.println("index_k=" + index.k());
        System.out.println("requested_k=" + k);
        System.out.println("cover_limit=" + coverLimit);
        System.out.println("decomposition_timeout_ms=" + decompositionTimeoutMs);

        int rows = 0;
        int queryNumber = 0;
        try (BufferedReader reader = Files.newBufferedReader(queriesPath, StandardCharsets.UTF_8);
                BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            writer.write("query,method,candidate_count,decompose_ms,timed_out,edges,query_text");
            writer.newLine();

            for (String line; (line = reader.readLine()) != null;) {
                String queryText = line.trim();
                if (queryText.isEmpty() || queryText.startsWith("#")) {
                    continue;
                }
                queryNumber++;
                ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
                rows += writeMethodRow(
                        writer,
                        planner,
                        queryNumber,
                        queryText,
                        query,
                        DecompositionMethod.COST,
                        coverLimit,
                        k,
                        decompositionTimeoutMs);
                rows += writeMethodRow(
                        writer,
                        planner,
                        queryNumber,
                        queryText,
                        query,
                        DecompositionMethod.MAX_COLLAPSE,
                        coverLimit,
                        k,
                        decompositionTimeoutMs);
            }
        }

        System.out.println("cover_limit_probe_complete");
        System.out.println("queries_processed=" + queryNumber);
        System.out.println("rows_written=" + rows);
        assertTrue(rows > 0, "Expected at least one probe row");
    }

    private static int writeMethodRow(
            BufferedWriter writer,
            Planner planner,
            int queryNumber,
            String queryText,
            ConjunctiveQuery query,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) throws Exception {
        Planner.MethodSelection selection = planner.planMethod(query, method, coverLimit, k, decompositionTimeoutMs);
        int candidateCount = selection.candidates().size();
        double decomposeMs = selection.decomposeNanos() / 1_000_000.0D;
        writer.write(String.format(
                Locale.ROOT,
                "%d,%s,%d,%.3f,%s,%d,\"%s\"",
                queryNumber,
                method.name(),
                candidateCount,
                decomposeMs,
                selection.timedOut(),
                query.edges().size(),
                queryText.replace("\"", "'")));
        writer.newLine();
        System.out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s candidate_count=%d decompose_ms=%.3f timed_out=%s edges=%d",
                queryNumber,
                method.name(),
                candidateCount,
                decomposeMs,
                selection.timedOut(),
                query.edges().size()));
        return 1;
    }
}
