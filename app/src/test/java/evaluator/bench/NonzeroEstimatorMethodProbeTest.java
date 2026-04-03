package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.bench.BenchTypes.CompareFileSpec;
import evaluator.bench.BenchTypes.CompareFileReport;
import evaluator.bench.BenchTypes.EvaluationMode;
import evaluator.evaluation.DecompositionMethod;
import evaluator.index.NativeCpqIndex;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Manual warmed compare-file probe over a mixed-shape nonzero workload slice.
 * It reuses the reported final_nonzero i50 workload and summarizes the current
 * cover-only and order-only estimation methods on CondMat and Gnutella.
 */
class NonzeroEstimatorMethodProbeTest {
    private static final String ENABLE_PROPERTY = "cpq.estimatorMethodProbe.enabled";
    private static final String WORKLOAD_DIR_PROPERTY = "cpq.estimatorMethodProbe.workloadDir";
    private static final String OUTPUT_DIR_PROPERTY = "cpq.estimatorMethodProbe.outputDir";
    private static final String DATASETS_PROPERTY = "cpq.estimatorMethodProbe.datasets";
    private static final String MEASURED_COUNT_PROPERTY = "cpq.estimatorMethodProbe.measuredCount";
    private static final String WARMUP_COUNT_PROPERTY = "cpq.estimatorMethodProbe.warmupCount";
    private static final String METHOD_TIMEOUT_PROPERTY = "cpq.estimatorMethodProbe.methodTimeoutMs";
    private static final String DECOMPOSITION_TIMEOUT_PROPERTY = "cpq.estimatorMethodProbe.decompositionTimeoutMs";
    private static final String COVER_LIMIT_PROPERTY = "cpq.estimatorMethodProbe.coverLimit";
    private static final String K_PROPERTY = "cpq.estimatorMethodProbe.k";
    private static final String SEED_PROPERTY = "cpq.estimatorMethodProbe.seed";
    private static final String METHODS_PROPERTY = "cpq.decompose.methods";

    private static final Path DEFAULT_WORKLOAD_DIR = Path.of(
            "local/queries/topology-bench/workloads/"
                    + "final_uniform8_shared_i50_all_label8_dir16_final_nonzero_wheel4_partial_20260323_114919");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("logs/nonzero_estimator_method_probe_20260324");
    private static final String DEFAULT_DATASETS = "ca-CondMat,p2p-Gnutella31";
    private static final int DEFAULT_MEASURED_COUNT = 100;
    private static final int DEFAULT_WARMUP_COUNT = 10;
    private static final int DEFAULT_METHOD_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_DECOMPOSITION_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_COVER_LIMIT = 10;
    private static final int DEFAULT_K = 2;
    private static final long DEFAULT_SEED = 21_919_306_292L;
    private static final String METHODS_VALUE = String.join(
            ",",
            "single_edge",
            "single_edge_system_r",
            "cost",
            "cost_system_r_order_only",
            "exhaustive_system_r_cover_only",
            "series_parallel",
            "series_parallel_system_r_order_only",
            "series_parallel_system_r_cover_only");

    private static final Pattern KEY_VALUE_PATTERN =
            Pattern.compile("([a-zA-Z0-9_]+)=((?:\"[^\"]*\")|(?:[^ ]+))");

    private static final List<PairComparison> METHOD_PAIRS = List.of(
            new PairComparison(
                    "cover_exact",
                    "cover",
                    DecompositionMethod.COST,
                    DecompositionMethod.EXHAUSTIVE_SYSTEM_R_COVER_ONLY),
            new PairComparison(
                    "cover_series_parallel",
                    "cover",
                    DecompositionMethod.SERIES_PARALLEL,
                    DecompositionMethod.SERIES_PARALLEL_SYSTEM_R_COVER_ONLY),
            new PairComparison(
                    "order_single_edge",
                    "order",
                    DecompositionMethod.SINGLE_EDGE,
                    DecompositionMethod.SINGLE_EDGE_SYSTEM_R),
            new PairComparison(
                    "order_cost",
                    "order",
                    DecompositionMethod.COST,
                    DecompositionMethod.COST_SYSTEM_R_ORDER_ONLY),
            new PairComparison(
                    "order_series_parallel",
                    "order",
                    DecompositionMethod.SERIES_PARALLEL,
                    DecompositionMethod.SERIES_PARALLEL_SYSTEM_R_ORDER_ONLY));

    @Test
    void runMixedShapeNonzeroEstimatorProbe() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY), "Manual probe disabled");

        Path workloadDir = Path.of(System.getProperty(WORKLOAD_DIR_PROPERTY, DEFAULT_WORKLOAD_DIR.toString()));
        Path outputDir = Path.of(System.getProperty(OUTPUT_DIR_PROPERTY, DEFAULT_OUTPUT_DIR.toString()));
        List<String> datasets = parseDatasets(System.getProperty(DATASETS_PROPERTY, DEFAULT_DATASETS));
        int measuredCount = Integer.getInteger(MEASURED_COUNT_PROPERTY, DEFAULT_MEASURED_COUNT);
        int warmupCount = Integer.getInteger(WARMUP_COUNT_PROPERTY, DEFAULT_WARMUP_COUNT);
        int methodTimeoutMs = Integer.getInteger(METHOD_TIMEOUT_PROPERTY, DEFAULT_METHOD_TIMEOUT_MS);
        int decompositionTimeoutMs = Integer.getInteger(
                DECOMPOSITION_TIMEOUT_PROPERTY,
                DEFAULT_DECOMPOSITION_TIMEOUT_MS);
        int coverLimit = Integer.getInteger(COVER_LIMIT_PROPERTY, DEFAULT_COVER_LIMIT);
        int k = Integer.getInteger(K_PROPERTY, DEFAULT_K);
        long seed = Long.getLong(SEED_PROPERTY, DEFAULT_SEED);

        Path queryMapPath = workloadDir.resolve("topology-bench.a123_i50.query_map.csv");
        Path queriesPath = workloadDir.resolve("topology-bench.a123_i50.cq");
        Assumptions.assumeTrue(Files.exists(queryMapPath), "Missing query map " + queryMapPath);
        Assumptions.assumeTrue(Files.exists(queriesPath), "Missing workload queries " + queriesPath);

        List<QuerySpec> allQueries = loadQueries(queryMapPath, queriesPath);
        Assumptions.assumeTrue(
                allQueries.size() >= measuredCount + warmupCount,
                "Insufficient workload size for requested sample");

        List<QuerySpec> measuredQueries = selectMeasuredQueries(allQueries, measuredCount);
        List<QuerySpec> warmupQueries = selectWarmupQueries(allQueries, measuredQueries, warmupCount);

        Files.createDirectories(outputDir);
        Path measuredFile = outputDir.resolve("measured_100_mixed_shapes.cq");
        Path warmupFile = outputDir.resolve("warmup_10_mixed_shapes.cq");
        Path selectionPath = outputDir.resolve("selected_queries.csv");
        writeQueryFile(measuredQueries, measuredFile);
        writeQueryFile(warmupQueries, warmupFile);
        writeSelectionCsv(selectionPath, measuredQueries, warmupQueries);

        Map<Integer, QuerySpec> measuredByOrdinal = indexByOrdinal(measuredQueries);

        Path overallSummaryPath = outputDir.resolve("overall_summary.txt");
        Path overallPerMethodPath = outputDir.resolve("overall_per_method.csv");
        Path overallPairPath = outputDir.resolve("overall_pair_summary.csv");
        Path overallFamilyPairPath = outputDir.resolve("overall_family_pair_summary.csv");

        System.out.println("nonzero_estimator_method_probe_start");
        System.out.println("workload_dir=" + workloadDir.toAbsolutePath());
        System.out.println("output_dir=" + outputDir.toAbsolutePath());
        System.out.println("datasets=" + datasets);
        System.out.println("measured_queries=" + measuredQueries.size());
        System.out.println("warmup_queries=" + warmupQueries.size());
        System.out.println("method_timeout_ms=" + methodTimeoutMs);
        System.out.println("decomposition_timeout_ms=" + decompositionTimeoutMs);
        System.out.println("cover_limit=" + coverLimit);
        System.out.println("k=" + k);
        System.out.println("seed=" + seed);

        String previousMethods = System.getProperty(METHODS_PROPERTY);
        System.setProperty(METHODS_PROPERTY, METHODS_VALUE);
        try {
            CombinedAccumulator combined = new CombinedAccumulator();
            try (BufferedWriter overallSummaryWriter =
                            Files.newBufferedWriter(overallSummaryPath, StandardCharsets.UTF_8);
                    BufferedWriter overallMethodWriter =
                            Files.newBufferedWriter(overallPerMethodPath, StandardCharsets.UTF_8);
                    BufferedWriter overallPairWriter =
                            Files.newBufferedWriter(overallPairPath, StandardCharsets.UTF_8);
                    BufferedWriter overallFamilyPairWriter =
                            Files.newBufferedWriter(overallFamilyPairPath, StandardCharsets.UTF_8)) {
                overallMethodWriter.write(
                        "dataset,method,rows,ok_rows,exec_timeout_rows,planning_timeout_rows,no_candidate_rows,error_rows,"
                                + "avg_method_wall_ms,median_method_wall_ms,avg_planning_ms,median_planning_ms,"
                                + "avg_execution_ms,median_execution_ms,avg_join_order_ms,median_join_order_ms");
                overallMethodWriter.newLine();
                overallPairWriter.write(
                        "dataset,pair_id,pair_kind,baseline_method,estimate_method,both_ok_queries,estimate_faster_queries,"
                                + "baseline_timeout_rows,estimate_timeout_rows,avg_delta_ms,median_delta_ms,avg_speedup_ratio");
                overallPairWriter.newLine();
                overallFamilyPairWriter.write(
                        "dataset,family,pair_id,pair_kind,both_ok_queries,estimate_faster_queries,avg_delta_ms,median_delta_ms");
                overallFamilyPairWriter.newLine();

                overallSummaryWriter.write("started=" + Instant.now());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("workload_dir=" + workloadDir.toAbsolutePath());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("measured_file=" + measuredFile.toAbsolutePath());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("warmup_file=" + warmupFile.toAbsolutePath());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("datasets=" + datasets);
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("methods=" + METHODS_VALUE);
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("measured_queries=" + measuredQueries.size());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("warmup_queries=" + warmupQueries.size());
                overallSummaryWriter.newLine();
                overallSummaryWriter.write("seed=" + seed);
                overallSummaryWriter.newLine();
                overallSummaryWriter.newLine();

                for (String dataset : datasets) {
                    DatasetRunResult result = runDataset(
                            dataset,
                            outputDir,
                            measuredFile,
                            warmupFile,
                            measuredByOrdinal,
                            warmupQueries.size(),
                            methodTimeoutMs,
                            decompositionTimeoutMs,
                            coverLimit,
                            k,
                            seed);
                    writeDatasetSummary(
                            overallSummaryWriter,
                            result,
                            overallMethodWriter,
                            overallPairWriter,
                            overallFamilyPairWriter);
                    combined.add(result);
                }

                writeCombinedSummary(overallSummaryWriter, combined);
            }
        } finally {
            if (previousMethods == null) {
                System.clearProperty(METHODS_PROPERTY);
            } else {
                System.setProperty(METHODS_PROPERTY, previousMethods);
            }
        }

        System.out.println("nonzero_estimator_method_probe_complete");
        System.out.println("selection_csv=" + selectionPath.toAbsolutePath());
        System.out.println("overall_summary=" + overallSummaryPath.toAbsolutePath());
        System.out.println("overall_per_method=" + overallPerMethodPath.toAbsolutePath());
        System.out.println("overall_pair_summary=" + overallPairPath.toAbsolutePath());
        System.out.println("overall_family_pair_summary=" + overallFamilyPairPath.toAbsolutePath());
        assertTrue(Files.size(overallPerMethodPath) > 0L, "Expected method summary output");
    }

    private static DatasetRunResult runDataset(
            String dataset,
            Path outputDir,
            Path measuredFile,
            Path warmupFile,
            Map<Integer, QuerySpec> measuredByOrdinal,
            int warmupCount,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int k,
            long seed) throws Exception {
        Path datasetDir = outputDir.resolve(dataset);
        Files.createDirectories(datasetDir);

        Path indexPath = Path.of("local/indices/final_uniform8/fork_" + dataset + ".idx");
        Assumptions.assumeTrue(Files.exists(indexPath), "Missing dataset index " + indexPath);

        Path compareLogPath = datasetDir.resolve("compare.log");
        Path decompositionLogPath = datasetDir.resolve("decompositions.log");
        Path perMethodPath = datasetDir.resolve("per_method_summary.csv");
        Path pairSummaryPath = datasetDir.resolve("pair_summary.csv");
        Path familyPairPath = datasetDir.resolve("family_pair_summary.csv");
        Path datasetSummaryPath = datasetDir.resolve("summary.txt");

        NativeCpqIndex index = NativeCpqIndex.load(indexPath);
        BenchRunner runner = new BenchRunner(index, EngineConfig.defaults());
        CompareFileSpec spec = new CompareFileSpec(
                indexPath,
                measuredFile,
                warmupFile,
                warmupCount,
                EvaluationMode.ROWS,
                methodTimeoutMs,
                decompositionTimeoutMs,
                coverLimit,
                k,
                seed,
                compareLogPath,
                decompositionLogPath,
                true);
        CompareFileReport report = runner.compareFile(spec);

        List<CompareRow> rows = parseCompareLog(compareLogPath);
        MethodSummaryMap methodSummaries = summarizeMethods(rows);
        PairSummaryMap pairSummaries = summarizePairs(rows, measuredByOrdinal);
        FamilyPairSummaryMap familyPairSummaries = summarizeFamilyPairs(rows, measuredByOrdinal);

        try (BufferedWriter methodWriter = Files.newBufferedWriter(perMethodPath, StandardCharsets.UTF_8);
                BufferedWriter pairWriter = Files.newBufferedWriter(pairSummaryPath, StandardCharsets.UTF_8);
                BufferedWriter familyWriter = Files.newBufferedWriter(familyPairPath, StandardCharsets.UTF_8);
                BufferedWriter summaryWriter = Files.newBufferedWriter(datasetSummaryPath, StandardCharsets.UTF_8)) {
            methodWriter.write(
                    "method,rows,ok_rows,exec_timeout_rows,planning_timeout_rows,no_candidate_rows,error_rows,"
                            + "avg_method_wall_ms,median_method_wall_ms,avg_planning_ms,median_planning_ms,"
                            + "avg_execution_ms,median_execution_ms,avg_join_order_ms,median_join_order_ms");
            methodWriter.newLine();
            for (MethodAggregate aggregate : methodSummaries.ordered()) {
                methodWriter.write(aggregate.toCsvRow(null));
                methodWriter.newLine();
            }

            pairWriter.write(
                    "pair_id,pair_kind,baseline_method,estimate_method,both_ok_queries,estimate_faster_queries,"
                            + "baseline_timeout_rows,estimate_timeout_rows,avg_delta_ms,median_delta_ms,avg_speedup_ratio");
            pairWriter.newLine();
            for (PairAggregate aggregate : pairSummaries.ordered()) {
                pairWriter.write(aggregate.toCsvRow(null));
                pairWriter.newLine();
            }

            familyWriter.write(
                    "family,pair_id,pair_kind,both_ok_queries,estimate_faster_queries,avg_delta_ms,median_delta_ms");
            familyWriter.newLine();
            for (FamilyPairAggregate aggregate : familyPairSummaries.ordered()) {
                familyWriter.write(aggregate.toCsvRow(null));
                familyWriter.newLine();
            }

            summaryWriter.write("dataset=" + dataset);
            summaryWriter.newLine();
            summaryWriter.write("index=" + indexPath.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("compare_log=" + compareLogPath.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("decompositions_log=" + decompositionLogPath.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("report_query_count=" + report.queryCount());
            summaryWriter.newLine();
            summaryWriter.write("report_method_rows=" + report.methodRows());
            summaryWriter.newLine();
            summaryWriter.write("report_ok_rows=" + report.okRows());
            summaryWriter.newLine();
            summaryWriter.write("report_exec_timeout_rows=" + report.timeoutRows());
            summaryWriter.newLine();
            summaryWriter.write("report_planning_timeout_rows=" + report.decompositionTimeoutRows());
            summaryWriter.newLine();
            summaryWriter.write("report_no_candidate_rows=" + report.noCandidateRows());
            summaryWriter.newLine();
            summaryWriter.write("report_error_rows=" + report.errorRows());
            summaryWriter.newLine();
            summaryWriter.write(String.format(
                    Locale.ROOT,
                    "report_elapsed_ms=%.3f",
                    report.elapsedNanos() / 1_000_000.0d));
            summaryWriter.newLine();
        }

        assertEquals(100, report.queryCount(), "Expected 100 measured queries for " + dataset);
        return new DatasetRunResult(
                dataset,
                report,
                methodSummaries,
                pairSummaries,
                familyPairSummaries,
                compareLogPath,
                decompositionLogPath,
                datasetSummaryPath);
    }

    private static List<QuerySpec> loadQueries(Path queryMapPath, Path queriesPath) throws Exception {
        List<String> queryLines = Files.readAllLines(queriesPath, StandardCharsets.UTF_8);
        List<String> metadataLines = Files.readAllLines(queryMapPath, StandardCharsets.UTF_8);
        assertEquals(queryLines.size() + 1, metadataLines.size(), "Query/metadata size mismatch");
        String[] header = metadataLines.get(0).split(",", -1);

        List<QuerySpec> queries = new ArrayList<>(queryLines.size());
        for (int i = 1; i < metadataLines.size(); i++) {
            Map<String, String> columns = parseCsvRow(header, metadataLines.get(i));
            queries.add(new QuerySpec(
                    Integer.parseInt(columns.get("query_id")),
                    queryLines.get(i - 1),
                    columns.get("family"),
                    columns.get("template"),
                    Integer.parseInt(columns.get("arity")),
                    Integer.parseInt(columns.get("injection_rank")),
                    columns.get("labels"),
                    columns.get("direction_pattern"),
                    columns.get("head_vars")));
        }
        return List.copyOf(queries);
    }

    private static List<QuerySpec> selectMeasuredQueries(List<QuerySpec> allQueries, int targetCount) {
        Map<String, List<QuerySpec>> byTemplate = new TreeMap<>();
        for (QuerySpec query : allQueries) {
            byTemplate.computeIfAbsent(query.template(), key -> new ArrayList<>()).add(query);
        }

        List<QuerySpec> selected = new ArrayList<>(targetCount);
        Set<Integer> used = new LinkedHashSet<>();
        int round = 0;
        while (selected.size() < targetCount) {
            boolean progressed = false;
            for (Map.Entry<String, List<QuerySpec>> entry : byTemplate.entrySet()) {
                List<QuerySpec> templateQueries = entry.getValue();
                if (round >= templateQueries.size()) {
                    continue;
                }
                QuerySpec candidate = templateQueries.get(round);
                if (used.add(candidate.queryId())) {
                    selected.add(candidate);
                    progressed = true;
                    if (selected.size() == targetCount) {
                        break;
                    }
                }
            }
            if (!progressed) {
                break;
            }
            round++;
        }
        return List.copyOf(selected);
    }

    private static List<QuerySpec> selectWarmupQueries(
            List<QuerySpec> allQueries,
            List<QuerySpec> measuredQueries,
            int warmupCount) {
        Set<Integer> used = new LinkedHashSet<>();
        for (QuerySpec query : measuredQueries) {
            used.add(query.queryId());
        }

        Map<String, List<QuerySpec>> byFamily = new TreeMap<>();
        for (QuerySpec query : allQueries) {
            if (!used.contains(query.queryId())) {
                byFamily.computeIfAbsent(query.family(), key -> new ArrayList<>()).add(query);
            }
        }

        List<QuerySpec> warmups = new ArrayList<>(warmupCount);
        for (Map.Entry<String, List<QuerySpec>> entry : byFamily.entrySet()) {
            if (warmups.size() == warmupCount) {
                break;
            }
            QuerySpec candidate = entry.getValue().get(0);
            if (used.add(candidate.queryId())) {
                warmups.add(candidate);
            }
        }
        if (warmups.size() < warmupCount) {
            for (QuerySpec query : allQueries) {
                if (warmups.size() == warmupCount) {
                    break;
                }
                if (used.add(query.queryId())) {
                    warmups.add(query);
                }
            }
        }
        return List.copyOf(warmups);
    }

    private static void writeQueryFile(List<QuerySpec> queries, Path path) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (QuerySpec query : queries) {
                writer.write(query.queryText());
                writer.newLine();
            }
        }
    }

    private static void writeSelectionCsv(Path path, List<QuerySpec> measured, List<QuerySpec> warmup) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("set,ordinal,query_id,family,template,arity,injection_rank,labels,direction_pattern,head_vars");
            writer.newLine();
            writeSelectionRows(writer, "measured", measured);
            writeSelectionRows(writer, "warmup", warmup);
        }
    }

    private static void writeSelectionRows(BufferedWriter writer, String setId, List<QuerySpec> queries) throws Exception {
        int ordinal = 0;
        for (QuerySpec query : queries) {
            ordinal++;
            writer.write(String.format(
                    Locale.ROOT,
                    "%s,%d,%d,%s,%s,%d,%d,%s,%s,%s",
                    setId,
                    ordinal,
                    query.queryId(),
                    csv(query.family()),
                    csv(query.template()),
                    query.arity(),
                    query.injectionRank(),
                    csv(query.labels()),
                    csv(query.directionPattern()),
                    csv(query.headVars())));
            writer.newLine();
        }
    }

    private static Map<Integer, QuerySpec> indexByOrdinal(List<QuerySpec> measuredQueries) {
        Map<Integer, QuerySpec> indexed = new HashMap<>();
        int ordinal = 0;
        for (QuerySpec query : measuredQueries) {
            ordinal++;
            indexed.put(ordinal, query);
        }
        return indexed;
    }

    private static List<CompareRow> parseCompareLog(Path compareLogPath) throws Exception {
        List<CompareRow> rows = new ArrayList<>();
        for (String line : Files.readAllLines(compareLogPath, StandardCharsets.UTF_8)) {
            if (!line.startsWith("query=") || !line.contains(" method=") || !line.contains(" status=")) {
                continue;
            }
            Map<String, String> fields = parseKeyValueLine(line);
            String methodToken = fields.get("method");
            if (methodToken == null) {
                continue;
            }
            rows.add(new CompareRow(
                    Integer.parseInt(fields.get("query")),
                    DecompositionMethod.valueOf(methodToken),
                    fields.get("status"),
                    doubleField(fields, "planning_ms"),
                    doubleField(fields, "method_wall_ms"),
                    doubleField(fields, "execution_ms"),
                    doubleField(fields, "join_order_ms"),
                    doubleField(fields, "join_ms"),
                    longField(fields, "answers")));
        }
        return List.copyOf(rows);
    }

    private static MethodSummaryMap summarizeMethods(List<CompareRow> rows) {
        EnumMap<DecompositionMethod, MethodAggregate> byMethod = new EnumMap<>(DecompositionMethod.class);
        for (CompareRow row : rows) {
            byMethod.computeIfAbsent(row.method(), MethodAggregate::new).add(row);
        }
        return new MethodSummaryMap(byMethod);
    }

    private static PairSummaryMap summarizePairs(List<CompareRow> rows, Map<Integer, QuerySpec> measuredByOrdinal) {
        Map<QueryMethodKey, CompareRow> byQueryAndMethod = new HashMap<>();
        for (CompareRow row : rows) {
            byQueryAndMethod.put(new QueryMethodKey(row.queryNumber(), row.method()), row);
        }

        Map<String, PairAggregate> aggregates = new LinkedHashMap<>();
        for (PairComparison pair : METHOD_PAIRS) {
            PairAggregate aggregate = new PairAggregate(pair);
            for (int queryNumber : measuredByOrdinal.keySet()) {
                CompareRow baseline = byQueryAndMethod.get(new QueryMethodKey(queryNumber, pair.baselineMethod()));
                CompareRow estimate = byQueryAndMethod.get(new QueryMethodKey(queryNumber, pair.estimateMethod()));
                aggregate.add(baseline, estimate);
            }
            aggregates.put(pair.id(), aggregate);
        }
        return new PairSummaryMap(aggregates);
    }

    private static FamilyPairSummaryMap summarizeFamilyPairs(
            List<CompareRow> rows,
            Map<Integer, QuerySpec> measuredByOrdinal) {
        Map<QueryMethodKey, CompareRow> byQueryAndMethod = new HashMap<>();
        for (CompareRow row : rows) {
            byQueryAndMethod.put(new QueryMethodKey(row.queryNumber(), row.method()), row);
        }

        Map<String, FamilyPairAggregate> byFamilyAndPair = new LinkedHashMap<>();
        for (PairComparison pair : METHOD_PAIRS) {
            for (Map.Entry<Integer, QuerySpec> entry : measuredByOrdinal.entrySet()) {
                CompareRow baseline = byQueryAndMethod.get(new QueryMethodKey(entry.getKey(), pair.baselineMethod()));
                CompareRow estimate = byQueryAndMethod.get(new QueryMethodKey(entry.getKey(), pair.estimateMethod()));
                String key = entry.getValue().family() + "|" + pair.id();
                byFamilyAndPair
                        .computeIfAbsent(key, unused -> new FamilyPairAggregate(entry.getValue().family(), pair))
                        .add(baseline, estimate);
            }
        }
        return new FamilyPairSummaryMap(byFamilyAndPair);
    }

    private static void writeDatasetSummary(
            BufferedWriter summaryWriter,
            DatasetRunResult result,
            BufferedWriter overallMethodWriter,
            BufferedWriter overallPairWriter,
            BufferedWriter overallFamilyPairWriter) throws Exception {
        summaryWriter.write("dataset=" + result.dataset());
        summaryWriter.newLine();
        summaryWriter.write("compare_log=" + result.compareLogPath().toAbsolutePath());
        summaryWriter.newLine();
        summaryWriter.write("decompositions_log=" + result.decompositionLogPath().toAbsolutePath());
        summaryWriter.newLine();
        summaryWriter.write("dataset_summary=" + result.datasetSummaryPath().toAbsolutePath());
        summaryWriter.newLine();
        summaryWriter.write(String.format(
                Locale.ROOT,
                "report query_count=%d method_rows=%d ok=%d exec_timeout=%d planning_timeout=%d no_candidate=%d error=%d elapsed_ms=%.3f",
                result.report().queryCount(),
                result.report().methodRows(),
                result.report().okRows(),
                result.report().timeoutRows(),
                result.report().decompositionTimeoutRows(),
                result.report().noCandidateRows(),
                result.report().errorRows(),
                result.report().elapsedNanos() / 1_000_000.0d));
        summaryWriter.newLine();
        summaryWriter.newLine();

        for (MethodAggregate aggregate : result.methodSummaries().ordered()) {
            overallMethodWriter.write(aggregate.toCsvRow(result.dataset()));
            overallMethodWriter.newLine();
        }
        for (PairAggregate aggregate : result.pairSummaries().ordered()) {
            overallPairWriter.write(aggregate.toCsvRow(result.dataset()));
            overallPairWriter.newLine();
        }
        for (FamilyPairAggregate aggregate : result.familyPairSummaries().ordered()) {
            overallFamilyPairWriter.write(aggregate.toCsvRow(result.dataset()));
            overallFamilyPairWriter.newLine();
        }
    }

    private static void writeCombinedSummary(BufferedWriter summaryWriter, CombinedAccumulator combined) throws Exception {
        summaryWriter.write("combined");
        summaryWriter.newLine();
        for (PairAggregate aggregate : combined.combinedPairs().values()) {
            summaryWriter.write(String.format(
                    Locale.ROOT,
                    "pair=%s kind=%s both_ok=%d estimate_faster=%d avg_delta_ms=%.3f median_delta_ms=%.3f avg_speedup_ratio=%.3f",
                    aggregate.pair().id(),
                    aggregate.pair().kind(),
                    aggregate.bothOkQueries(),
                    aggregate.estimateFasterQueries(),
                    aggregate.deltaMsStats().average(),
                    aggregate.deltaMsStats().median(),
                    aggregate.speedupStats().average()));
            summaryWriter.newLine();
        }
    }

    private static Map<String, String> parseKeyValueLine(String line) {
        Map<String, String> fields = new HashMap<>();
        Matcher matcher = KEY_VALUE_PATTERN.matcher(line);
        while (matcher.find()) {
            fields.put(matcher.group(1), stripQuotes(matcher.group(2)));
        }
        return fields;
    }

    private static Map<String, String> parseCsvRow(String[] header, String line) {
        String[] parts = line.split(",", -1);
        Map<String, String> columns = new HashMap<>();
        for (int i = 0; i < header.length && i < parts.length; i++) {
            columns.put(header[i], parts[i]);
        }
        return columns;
    }

    private static String csv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"")) {
            return '"' + value.replace("\"", "\"\"") + '"';
        }
        return value;
    }

    private static String stripQuotes(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static double doubleField(Map<String, String> fields, String key) {
        String raw = fields.get(key);
        return raw == null || raw.isBlank() ? Double.NaN : Double.parseDouble(raw);
    }

    private static long longField(Map<String, String> fields, String key) {
        String raw = fields.get(key);
        return raw == null || raw.isBlank() ? -1L : Long.parseLong(raw);
    }

    private static List<String> parseDatasets(String raw) {
        List<String> datasets = new ArrayList<>();
        for (String part : raw.split(",")) {
            String dataset = part.trim();
            if (!dataset.isEmpty()) {
                datasets.add(dataset);
            }
        }
        return List.copyOf(datasets);
    }

    private record QuerySpec(
            int queryId,
            String queryText,
            String family,
            String template,
            int arity,
            int injectionRank,
            String labels,
            String directionPattern,
            String headVars) {
    }

    private record CompareRow(
            int queryNumber,
            DecompositionMethod method,
            String status,
            double planningMs,
            double methodWallMs,
            double executionMs,
            double joinOrderMs,
            double joinMs,
            long answers) {
        private boolean ok() {
            return "OK".equals(status);
        }

        private boolean timeout() {
            return "TIMEOUT".equals(status) || "EXEC_TIMEOUT".equals(status);
        }

        private boolean planningTimeout() {
            return "PLANNING_TIMEOUT".equals(status);
        }

        private boolean noCandidate() {
            return "NO_CANDIDATE".equals(status);
        }
    }

    private record QueryMethodKey(int queryNumber, DecompositionMethod method) {
    }

    private record PairComparison(
            String id,
            String kind,
            DecompositionMethod baselineMethod,
            DecompositionMethod estimateMethod) {
    }

    private static final class StatsAccumulator {
        private final List<Double> values = new ArrayList<>();

        private void add(double value) {
            if (!Double.isNaN(value) && !Double.isInfinite(value)) {
                values.add(value);
            }
        }

        private int size() {
            return values.size();
        }

        private double average() {
            if (values.isEmpty()) {
                return Double.NaN;
            }
            double sum = 0.0d;
            for (double value : values) {
                sum += value;
            }
            return sum / values.size();
        }

        private double median() {
            if (values.isEmpty()) {
                return Double.NaN;
            }
            List<Double> sorted = new ArrayList<>(values);
            sorted.sort(Comparator.naturalOrder());
            int mid = sorted.size() / 2;
            if ((sorted.size() & 1) == 1) {
                return sorted.get(mid);
            }
            return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0d;
        }
    }

    private static final class MethodAggregate {
        private final DecompositionMethod method;
        private int rows;
        private int okRows;
        private int execTimeoutRows;
        private int planningTimeoutRows;
        private int noCandidateRows;
        private int errorRows;
        private final StatsAccumulator methodWallMs = new StatsAccumulator();
        private final StatsAccumulator planningMs = new StatsAccumulator();
        private final StatsAccumulator executionMs = new StatsAccumulator();
        private final StatsAccumulator joinOrderMs = new StatsAccumulator();

        private MethodAggregate(DecompositionMethod method) {
            this.method = method;
        }

        private void add(CompareRow row) {
            rows++;
            if (row.ok()) {
                okRows++;
                methodWallMs.add(row.methodWallMs());
                planningMs.add(row.planningMs());
                executionMs.add(row.executionMs());
                joinOrderMs.add(row.joinOrderMs());
            } else if (row.timeout()) {
                execTimeoutRows++;
            } else if (row.planningTimeout()) {
                planningTimeoutRows++;
            } else if (row.noCandidate()) {
                noCandidateRows++;
            } else {
                errorRows++;
            }
        }

        private String toCsvRow(String dataset) {
            String prefix = dataset == null ? "" : csv(dataset) + ",";
            return String.format(
                    Locale.ROOT,
                    "%s%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
                    prefix,
                    method.name(),
                    rows,
                    okRows,
                    execTimeoutRows,
                    planningTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    methodWallMs.average(),
                    methodWallMs.median(),
                    planningMs.average(),
                    planningMs.median(),
                    executionMs.average(),
                    executionMs.median(),
                    joinOrderMs.average(),
                    joinOrderMs.median());
        }
    }

    private record MethodSummaryMap(EnumMap<DecompositionMethod, MethodAggregate> byMethod) {
        private List<MethodAggregate> ordered() {
            List<MethodAggregate> ordered = new ArrayList<>(byMethod.values());
            ordered.sort(Comparator.comparingInt(aggregate -> aggregate.method.ordinal()));
            return List.copyOf(ordered);
        }
    }

    private static final class PairAggregate {
        private final PairComparison pair;
        private int bothOkQueries;
        private int estimateFasterQueries;
        private int baselineTimeoutRows;
        private int estimateTimeoutRows;
        private final StatsAccumulator deltaMsStats = new StatsAccumulator();
        private final StatsAccumulator speedupStats = new StatsAccumulator();

        private PairAggregate(PairComparison pair) {
            this.pair = pair;
        }

        private void add(CompareRow baseline, CompareRow estimate) {
            if (baseline == null || estimate == null) {
                return;
            }
            if (baseline.timeout()) {
                baselineTimeoutRows++;
            }
            if (estimate.timeout()) {
                estimateTimeoutRows++;
            }
            if (!baseline.ok() || !estimate.ok()) {
                return;
            }
            bothOkQueries++;
            double deltaMs = estimate.methodWallMs() - baseline.methodWallMs();
            deltaMsStats.add(deltaMs);
            if (deltaMs < 0.0d) {
                estimateFasterQueries++;
            }
            if (estimate.methodWallMs() > 0.0d) {
                speedupStats.add(baseline.methodWallMs() / estimate.methodWallMs());
            }
        }

        private void mergeFrom(PairAggregate other) {
            bothOkQueries += other.bothOkQueries;
            estimateFasterQueries += other.estimateFasterQueries;
            baselineTimeoutRows += other.baselineTimeoutRows;
            estimateTimeoutRows += other.estimateTimeoutRows;
            deltaMsStats.values.addAll(other.deltaMsStats.values);
            speedupStats.values.addAll(other.speedupStats.values);
        }

        private String toCsvRow(String dataset) {
            String prefix = dataset == null ? "" : csv(dataset) + ",";
            return String.format(
                    Locale.ROOT,
                    "%s%s,%s,%s,%s,%d,%d,%d,%d,%.3f,%.3f,%.3f",
                    prefix,
                    pair.id(),
                    pair.kind(),
                    pair.baselineMethod().name(),
                    pair.estimateMethod().name(),
                    bothOkQueries,
                    estimateFasterQueries,
                    baselineTimeoutRows,
                    estimateTimeoutRows,
                    deltaMsStats.average(),
                    deltaMsStats.median(),
                    speedupStats.average());
        }

        private PairComparison pair() {
            return pair;
        }

        private int bothOkQueries() {
            return bothOkQueries;
        }

        private int estimateFasterQueries() {
            return estimateFasterQueries;
        }

        private StatsAccumulator deltaMsStats() {
            return deltaMsStats;
        }

        private StatsAccumulator speedupStats() {
            return speedupStats;
        }
    }

    private record PairSummaryMap(Map<String, PairAggregate> byPairId) {
        private List<PairAggregate> ordered() {
            List<PairAggregate> ordered = new ArrayList<>(byPairId.values());
            ordered.sort(Comparator.comparing(aggregate -> aggregate.pair().id()));
            return List.copyOf(ordered);
        }
    }

    private static final class FamilyPairAggregate {
        private final String family;
        private final PairComparison pair;
        private int bothOkQueries;
        private int estimateFasterQueries;
        private final StatsAccumulator deltaMsStats = new StatsAccumulator();

        private FamilyPairAggregate(String family, PairComparison pair) {
            this.family = family;
            this.pair = pair;
        }

        private void add(CompareRow baseline, CompareRow estimate) {
            if (baseline == null || estimate == null || !baseline.ok() || !estimate.ok()) {
                return;
            }
            bothOkQueries++;
            double deltaMs = estimate.methodWallMs() - baseline.methodWallMs();
            deltaMsStats.add(deltaMs);
            if (deltaMs < 0.0d) {
                estimateFasterQueries++;
            }
        }

        private String toCsvRow(String dataset) {
            String prefix = dataset == null ? "" : csv(dataset) + ",";
            return String.format(
                    Locale.ROOT,
                    "%s%s,%s,%s,%d,%d,%.3f,%.3f",
                    prefix,
                    csv(family),
                    pair.id(),
                    pair.kind(),
                    bothOkQueries,
                    estimateFasterQueries,
                    deltaMsStats.average(),
                    deltaMsStats.median());
        }
    }

    private record FamilyPairSummaryMap(Map<String, FamilyPairAggregate> byFamilyAndPair) {
        private List<FamilyPairAggregate> ordered() {
            List<FamilyPairAggregate> ordered = new ArrayList<>(byFamilyAndPair.values());
            ordered.sort(Comparator
                    .comparing((FamilyPairAggregate aggregate) -> aggregate.family)
                    .thenComparing(aggregate -> aggregate.pair.id()));
            return List.copyOf(ordered);
        }
    }

    private record DatasetRunResult(
            String dataset,
            CompareFileReport report,
            MethodSummaryMap methodSummaries,
            PairSummaryMap pairSummaries,
            FamilyPairSummaryMap familyPairSummaries,
            Path compareLogPath,
            Path decompositionLogPath,
            Path datasetSummaryPath) {
    }

    private static final class CombinedAccumulator {
        private final Map<String, PairAggregate> combinedPairs = new LinkedHashMap<>();

        private void add(DatasetRunResult result) {
            for (PairAggregate aggregate : result.pairSummaries().ordered()) {
                combinedPairs
                        .computeIfAbsent(aggregate.pair().id(), unused -> new PairAggregate(aggregate.pair()))
                        .mergeFrom(aggregate);
            }
        }

        private Map<String, PairAggregate> combinedPairs() {
            return combinedPairs;
        }
    }
}
