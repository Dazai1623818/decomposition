package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.cpqindex.CanonForm;
import dev.roanh.cpqindex.Main;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan.Component;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Manual probe that compares live exhaustive enumeration under the current
 * normalized identity against a stronger canonical-core identity.
 */
class ExhaustiveIdentityPruningProbeTest {
    private static final String ENABLE_PROPERTY = "cpq.exhaustiveIdentityProbe.enabled";
    private static final String OUTPUT_DIR_PROPERTY = "cpq.exhaustiveIdentityProbe.outputDir";
    private static final String K_VALUES_PROPERTY = "cpq.exhaustiveIdentityProbe.kValues";
    private static final String WARMUP_ROUNDS_PROPERTY = "cpq.exhaustiveIdentityProbe.warmupRounds";
    private static final String MEASURED_ROUNDS_PROPERTY = "cpq.exhaustiveIdentityProbe.measuredRounds";
    private static final String QUERY_FILE_PROPERTY = "cpq.exhaustiveIdentityProbe.queryFile";
    private static final String QUERY_SET_NAME_PROPERTY = "cpq.exhaustiveIdentityProbe.querySetName";
    private static final String METADATA_FILE_PROPERTY = "cpq.exhaustiveIdentityProbe.metadataFile";

    private static final Path DEFAULT_OUTPUT_DIR = Path.of("logs/exhaustive_identity_probe_20260406");
    private static final String DEFAULT_K_VALUES = "2,3";
    private static final int DEFAULT_WARMUP_ROUNDS = 1;
    private static final int DEFAULT_MEASURED_ROUNDS = 5;
    private static final int TEXT_PREVIEW_LIMIT = 96;

    @Test
    void runLiveEnumerationIdentityProbe() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY), "Manual probe disabled");

        Path outputDir = Path.of(System.getProperty(OUTPUT_DIR_PROPERTY, DEFAULT_OUTPUT_DIR.toString()));
        List<Integer> kValues = parseIntegers(System.getProperty(K_VALUES_PROPERTY, DEFAULT_K_VALUES));
        int warmupRounds = Integer.getInteger(WARMUP_ROUNDS_PROPERTY, DEFAULT_WARMUP_ROUNDS);
        int measuredRounds = Integer.getInteger(MEASURED_ROUNDS_PROPERTY, DEFAULT_MEASURED_ROUNDS);
        if (warmupRounds < 0 || measuredRounds <= 0) {
            throw new IllegalArgumentException("warmupRounds must be >= 0 and measuredRounds must be > 0");
        }

        List<QuerySetSpec> querySets = defaultQuerySets();
        for (QuerySetSpec querySet : querySets) {
            Assumptions.assumeTrue(Files.exists(querySet.queriesPath()), "Missing queries file " + querySet.queriesPath());
            if (querySet.metadataPath() != null) {
                Assumptions.assumeTrue(Files.exists(querySet.metadataPath()), "Missing metadata file " + querySet.metadataPath());
            }
        }

        Main.loadNatives();
        Files.createDirectories(outputDir);
        Path perQueryPath = outputDir.resolve("per_query.csv");
        Path perCategoryPath = outputDir.resolve("per_category.csv");
        Path summaryPath = outputDir.resolve("run_summary.txt");

        System.out.println("exhaustive_identity_probe_start");
        System.out.println("output_dir=" + outputDir.toAbsolutePath());
        System.out.println("k_values=" + kValues);
        System.out.println("warmup_rounds=" + warmupRounds);
        System.out.println("measured_rounds=" + measuredRounds);

        int queryRows = 0;
        int categoryRows = 0;
        try (BufferedWriter perQueryWriter = Files.newBufferedWriter(perQueryPath, StandardCharsets.UTF_8);
                BufferedWriter perCategoryWriter = Files.newBufferedWriter(perCategoryPath, StandardCharsets.UTF_8);
                BufferedWriter summaryWriter = Files.newBufferedWriter(summaryPath, StandardCharsets.UTF_8)) {
            perQueryWriter.write(
                    "query_set,k,query_ordinal,query_id,category,family,edges,"
                            + "normalized_wall_ms,canonical_wall_ms,canonical_over_normalized_wall,"
                            + "normalized_normalize_ms,canonical_normalize_ms,"
                            + "normalized_identity_ms,canonical_identity_ms,"
                            + "normalized_attempts,canonical_attempts,attempt_reduction,attempt_reduction_pct,"
                            + "normalized_expanded,canonical_expanded,expanded_reduction,expanded_reduction_pct,"
                            + "normalized_components,canonical_components,component_reduction,component_reduction_pct,"
                            + "canonical_faster,query_text");
            perQueryWriter.newLine();
            perCategoryWriter.write(
                    "query_set,k,category,queries,canonical_faster_queries,"
                            + "avg_canonical_over_normalized_wall,avg_attempt_reduction_pct,"
                            + "avg_expanded_reduction_pct,avg_component_reduction_pct,"
                            + "max_component_reduction_pct,max_component_reduction_query_id");
            perCategoryWriter.newLine();

            summaryWriter.write("started=" + Instant.now());
            summaryWriter.newLine();
            summaryWriter.write("output_dir=" + outputDir.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("k_values=" + kValues);
            summaryWriter.newLine();
            summaryWriter.write("warmup_rounds=" + warmupRounds);
            summaryWriter.newLine();
            summaryWriter.write("measured_rounds=" + measuredRounds);
            summaryWriter.newLine();
            summaryWriter.newLine();

            for (QuerySetSpec querySet : querySets) {
                List<QuerySpec> queries = readQueries(querySet);
                summaryWriter.write("query_set=" + querySet.name());
                summaryWriter.newLine();
                summaryWriter.write("queries_path=" + querySet.queriesPath().toAbsolutePath());
                summaryWriter.newLine();
                summaryWriter.write("query_count=" + queries.size());
                summaryWriter.newLine();
                if (querySet.metadataPath() != null) {
                    summaryWriter.write("metadata_path=" + querySet.metadataPath().toAbsolutePath());
                    summaryWriter.newLine();
                }

                for (int k : kValues) {
                    List<QueryResult> results = new ArrayList<>(queries.size());
                    for (QuerySpec querySpec : queries) {
                        QueryResult result = benchmarkQuery(querySpec, k, warmupRounds, measuredRounds);
                        results.add(result);
                        writePerQueryRow(perQueryWriter, result);
                        queryRows++;
                    }

                    Map<String, CategoryAccumulator> byCategory = groupByCategory(results);
                    for (Map.Entry<String, CategoryAccumulator> entry : byCategory.entrySet()) {
                        writePerCategoryRow(perCategoryWriter, querySet.name(), k, entry.getKey(), entry.getValue());
                        categoryRows++;
                    }

                    writeSummary(summaryWriter, querySet.name(), k, results, byCategory);
                    printHighlights(querySet.name(), k, results, byCategory);
                }

                summaryWriter.newLine();
            }
        }

        System.out.println("exhaustive_identity_probe_complete");
        System.out.println("query_rows=" + queryRows);
        System.out.println("category_rows=" + categoryRows);
        assertTrue(queryRows > 0, "Expected at least one probe row");
    }

    private static List<QuerySetSpec> defaultQuerySets() {
        String queryFileRaw = System.getProperty(QUERY_FILE_PROPERTY);
        if (queryFileRaw != null && !queryFileRaw.isBlank()) {
            Path queriesPath = Path.of(queryFileRaw);
            String querySetName = System.getProperty(QUERY_SET_NAME_PROPERTY, queriesPath.getFileName().toString());
            Path metadataPath = resolveMetadataPath(queriesPath);
            return List.of(new QuerySetSpec(querySetName, queriesPath, metadataPath));
        }
        return List.of(
                new QuerySetSpec(
                        "synthetic_varied",
                        Path.of("local/tmp/exhaustive_identity_varied_20260406.cq"),
                        null),
                new QuerySetSpec(
                        "topology_combo_by_category",
                        Path.of("local/queries/topology-bench/workloads/combination_queries/viz/queries.by_category.10_combinations.cq"),
                        Path.of("local/queries/topology-bench/workloads/combination_queries/queries.metadata.csv")));
    }

    private static List<Integer> parseIntegers(String raw) {
        List<Integer> values = new ArrayList<>();
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values.add(Integer.parseInt(trimmed));
            }
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("No k values configured");
        }
        return values;
    }

    private static Path resolveMetadataPath(Path queriesPath) {
        String metadataRaw = System.getProperty(METADATA_FILE_PROPERTY);
        if (metadataRaw != null && !metadataRaw.isBlank()) {
            return Path.of(metadataRaw);
        }
        String fileName = queriesPath.getFileName().toString();
        if (!fileName.endsWith(".cq")) {
            return null;
        }
        Path sibling = queriesPath.resolveSibling(fileName.substring(0, fileName.length() - 3) + ".query_map.csv");
        return Files.exists(sibling) ? sibling : null;
    }

    private static List<QuerySpec> readQueries(QuerySetSpec querySet) throws Exception {
        QueryMetadata metadata = querySet.metadataPath() == null
                ? QueryMetadata.empty()
                : loadMetadata(querySet.metadataPath());
        List<QuerySpec> queries = new ArrayList<>();
        Map<String, String> pendingMetadata = new HashMap<>();
        int ordinal = 0;
        for (String rawLine : Files.readAllLines(querySet.queriesPath(), StandardCharsets.UTF_8)) {
            String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("#")) {
                pendingMetadata.putAll(parseCommentMetadata(line.substring(1).trim()));
                continue;
            }

            ordinal++;
            QueryTag metadataTag = metadata.tagFor(line, ordinal);
            String queryId = firstNonEmpty(
                    pendingMetadata.get("id"),
                    metadataTag == null ? null : metadataTag.queryId(),
                    Integer.toString(ordinal));
            String category = firstNonEmpty(
                    pendingMetadata.get("category"),
                    metadataTag == null ? null : metadataTag.categoryOrTemplate(),
                    "uncategorized");
            String family = firstNonEmpty(
                    pendingMetadata.get("family"),
                    metadataTag == null ? null : metadataTag.family(),
                    category);
            queries.add(new QuerySpec(querySet.name(), ordinal, queryId, category, family, line));
            pendingMetadata.clear();
        }
        metadata.validateAlignedRowCount(querySet.metadataPath(), ordinal);
        return List.copyOf(queries);
    }

    private static Map<String, String> parseCommentMetadata(String raw) {
        Map<String, String> values = new HashMap<>();
        for (String token : raw.split("\\s+")) {
            int idx = token.indexOf('=');
            if (idx <= 0 || idx >= token.length() - 1) {
                continue;
            }
            values.put(token.substring(0, idx), token.substring(idx + 1));
        }
        return values;
    }

    private static QueryMetadata loadMetadata(Path metadataPath) throws Exception {
        Map<String, QueryTag> metadataByText = new HashMap<>();
        List<QueryTag> tagsByOrdinal = new ArrayList<>();
        List<String> lines = Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            return QueryMetadata.empty();
        }

        List<String> header = parseCsvLine(lines.get(0));
        int queryIdIndex = optionalColumnIndex(header, "query_id");
        int categoryIndex = optionalColumnIndex(header, "category");
        int familyIndex = optionalColumnIndex(header, "family");
        int templateIndex = optionalColumnIndex(header, "template");
        int queryIndex = optionalColumnIndex(header, "query");

        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            List<String> row = parseCsvLine(line);
            QueryTag tag = new QueryTag(
                    valueAt(row, queryIdIndex),
                    valueAt(row, categoryIndex),
                    valueAt(row, familyIndex),
                    valueAt(row, templateIndex));
            tagsByOrdinal.add(tag);
            String query = valueAt(row, queryIndex);
            if (query != null && !query.isBlank()) {
                metadataByText.putIfAbsent(query, tag);
            }
        }
        return new QueryMetadata(Map.copyOf(metadataByText), List.copyOf(tagsByOrdinal), queryIndex < 0);
    }

    private static int columnIndex(List<String> header, String name) {
        for (int i = 0; i < header.size(); i++) {
            if (name.equals(header.get(i))) {
                return i;
            }
        }
        throw new IllegalArgumentException("Missing column " + name);
    }

    private static int optionalColumnIndex(List<String> header, String name) {
        for (int i = 0; i < header.size(); i++) {
            if (name.equals(header.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static String valueAt(List<String> row, int index) {
        if (index < 0 || index >= row.size()) {
            return null;
        }
        String value = row.get(index);
        return value == null || value.isBlank() ? null : value;
    }

    private static List<String> parseCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (ch == ',' && !quoted) {
                out.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        out.add(current.toString());
        return out;
    }

    private static String firstNonEmpty(String first, String second, String fallback) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return fallback;
    }

    private static QueryResult benchmarkQuery(QuerySpec querySpec, int k, int warmupRounds, int measuredRounds)
            throws Exception {
        ConjunctiveQuery query = ConjunctiveQuery.parse(querySpec.text());
        EnumMap<IdentityMode, ModeAccumulator> accumulators = new EnumMap<>(IdentityMode.class);
        EnumMap<IdentityMode, StructuralSnapshot> expectedSnapshots = new EnumMap<>(IdentityMode.class);
        IdentityMode[] modes = IdentityMode.values();

        for (int round = 0; round < warmupRounds + measuredRounds; round++) {
            int offset = round % modes.length;
            for (int slot = 0; slot < modes.length; slot++) {
                IdentityMode mode = modes[(offset + slot) % modes.length];
                long start = System.nanoTime();
                EnumerationResult result = new Enumerator(query, k, mode).enumerate();
                long wallNanos = System.nanoTime() - start;
                StructuralSnapshot snapshot = new StructuralSnapshot(
                        result.registerAttempts(),
                        result.expandedComponents(),
                        result.finalComponents(),
                        result.finalChecksum());
                if (round < warmupRounds) {
                    continue;
                }

                StructuralSnapshot expected = expectedSnapshots.get(mode);
                if (expected == null) {
                    expectedSnapshots.put(mode, snapshot);
                } else if (!expected.equals(snapshot)) {
                    throw new IllegalStateException("Non-deterministic probe result for " + querySpec.queryId() + " mode="
                            + mode + " expected=" + expected + " actual=" + snapshot);
                }

                accumulators.computeIfAbsent(mode, ignored -> new ModeAccumulator())
                        .add(wallNanos, result.normalizeNanos(), result.identityNanos(), snapshot);
            }
        }

        ModeStats normalized = accumulators.get(IdentityMode.NORMALIZED).finish(measuredRounds);
        ModeStats canonical = accumulators.get(IdentityMode.CANONICAL_CORE).finish(measuredRounds);
        return new QueryResult(querySpec, k, query.edges().size(), normalized, canonical);
    }

    private static Map<String, CategoryAccumulator> groupByCategory(List<QueryResult> results) {
        Map<String, CategoryAccumulator> byCategory = new LinkedHashMap<>();
        for (QueryResult result : results) {
            byCategory.computeIfAbsent(result.query().category(), ignored -> new CategoryAccumulator())
                    .add(result);
        }
        return byCategory;
    }

    private static void writePerQueryRow(BufferedWriter writer, QueryResult result) throws Exception {
        ModeStats normalized = result.normalized();
        ModeStats canonical = result.canonical();
        writer.write(String.format(
                Locale.ROOT,
                "%s,%d,%d,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%.2f,%d,%d,%d,%.2f,%d,%d,%d,%.2f,%s,\"%s\"",
                result.query().setName(),
                result.k(),
                result.query().ordinal(),
                csv(result.query().queryId()),
                csv(result.query().category()),
                csv(result.query().family()),
                result.edges(),
                normalized.wallMs(),
                canonical.wallMs(),
                canonical.wallMs() / normalized.wallMs(),
                normalized.normalizeMs(),
                canonical.normalizeMs(),
                normalized.identityMs(),
                canonical.identityMs(),
                normalized.snapshot().registerAttempts(),
                canonical.snapshot().registerAttempts(),
                normalized.snapshot().registerAttempts() - canonical.snapshot().registerAttempts(),
                reductionPct(
                        normalized.snapshot().registerAttempts(),
                        canonical.snapshot().registerAttempts()),
                normalized.snapshot().expandedComponents(),
                canonical.snapshot().expandedComponents(),
                normalized.snapshot().expandedComponents() - canonical.snapshot().expandedComponents(),
                reductionPct(
                        normalized.snapshot().expandedComponents(),
                        canonical.snapshot().expandedComponents()),
                normalized.snapshot().finalComponents(),
                canonical.snapshot().finalComponents(),
                normalized.snapshot().finalComponents() - canonical.snapshot().finalComponents(),
                reductionPct(
                        normalized.snapshot().finalComponents(),
                        canonical.snapshot().finalComponents()),
                canonical.wallMs() < normalized.wallMs(),
                csv(abbreviate(result.query().text()))));
        writer.newLine();
    }

    private static void writePerCategoryRow(
            BufferedWriter writer,
            String querySet,
            int k,
            String category,
            CategoryAccumulator accumulator) throws Exception {
        writer.write(String.format(
                Locale.ROOT,
                "%s,%d,%s,%d,%d,%.3f,%.2f,%.2f,%.2f,%.2f,%s",
                querySet,
                k,
                csv(category),
                accumulator.queryCount,
                accumulator.canonicalFasterQueries,
                accumulator.avgWallRatio(),
                accumulator.avgAttemptReductionPct(),
                accumulator.avgExpandedReductionPct(),
                accumulator.avgComponentReductionPct(),
                accumulator.maxComponentReductionPct,
                csv(accumulator.maxComponentReductionQueryId == null ? "-" : accumulator.maxComponentReductionQueryId)));
        writer.newLine();
    }

    private static void writeSummary(
            BufferedWriter writer,
            String querySet,
            int k,
            List<QueryResult> results,
            Map<String, CategoryAccumulator> byCategory) throws Exception {
        int canonicalFasterQueries = 0;
        double wallRatioSum = 0.0D;
        double componentReductionSum = 0.0D;
        for (QueryResult result : results) {
            if (result.canonical().wallMs() < result.normalized().wallMs()) {
                canonicalFasterQueries++;
            }
            wallRatioSum += result.canonical().wallMs() / result.normalized().wallMs();
            componentReductionSum += reductionPct(
                    result.normalized().snapshot().finalComponents(),
                    result.canonical().snapshot().finalComponents());
        }

        writer.write(String.format(
                Locale.ROOT,
                "set=%s k=%d queries=%d canonical_faster=%d avg_canonical_over_normalized_wall=%.3f avg_component_reduction_pct=%.2f",
                querySet,
                k,
                results.size(),
                canonicalFasterQueries,
                wallRatioSum / results.size(),
                componentReductionSum / results.size()));
        writer.newLine();

        for (Map.Entry<String, CategoryAccumulator> entry : byCategory.entrySet()) {
            CategoryAccumulator accumulator = entry.getValue();
            writer.write(String.format(
                    Locale.ROOT,
                    "  category=%s queries=%d canonical_faster=%d avg_wall_ratio=%.3f avg_component_reduction_pct=%.2f max_component_reduction_pct=%.2f max_query=%s",
                    entry.getKey(),
                    accumulator.queryCount,
                    accumulator.canonicalFasterQueries,
                    accumulator.avgWallRatio(),
                    accumulator.avgComponentReductionPct(),
                    accumulator.maxComponentReductionPct,
                    accumulator.maxComponentReductionQueryId == null ? "-" : accumulator.maxComponentReductionQueryId));
            writer.newLine();
        }
        writer.newLine();
    }

    private static void printHighlights(
            String querySet,
            int k,
            List<QueryResult> results,
            Map<String, CategoryAccumulator> byCategory) {
        System.out.println(String.format(Locale.ROOT, "query_set=%s k=%d queries=%d", querySet, k, results.size()));
        results.stream()
                .sorted(Comparator.comparingDouble(ExhaustiveIdentityPruningProbeTest::componentReductionPct).reversed())
                .limit(3)
                .forEach(result -> System.out.println(String.format(
                        Locale.ROOT,
                        "top_component_reduction query=%s category=%s component_reduction_pct=%.2f wall_ratio=%.3f normalized_components=%d canonical_components=%d",
                        result.query().queryId(),
                        result.query().category(),
                        componentReductionPct(result),
                        result.canonical().wallMs() / result.normalized().wallMs(),
                        result.normalized().snapshot().finalComponents(),
                        result.canonical().snapshot().finalComponents())));
        results.stream()
                .sorted(Comparator.comparingDouble(result -> result.canonical().wallMs() / result.normalized().wallMs()))
                .limit(3)
                .forEach(result -> System.out.println(String.format(
                        Locale.ROOT,
                        "top_speedup query=%s category=%s wall_ratio=%.3f component_reduction_pct=%.2f expanded_reduction_pct=%.2f",
                        result.query().queryId(),
                        result.query().category(),
                        result.canonical().wallMs() / result.normalized().wallMs(),
                        componentReductionPct(result),
                        expandedReductionPct(result))));
        for (Map.Entry<String, CategoryAccumulator> entry : byCategory.entrySet()) {
            CategoryAccumulator accumulator = entry.getValue();
            System.out.println(String.format(
                    Locale.ROOT,
                    "category=%s canonical_faster=%d/%d avg_wall_ratio=%.3f avg_component_reduction_pct=%.2f",
                    entry.getKey(),
                    accumulator.canonicalFasterQueries,
                    accumulator.queryCount,
                    accumulator.avgWallRatio(),
                    accumulator.avgComponentReductionPct()));
        }
    }

    private static String abbreviate(String queryText) {
        String sanitized = queryText.replace('"', '\'');
        if (sanitized.length() <= TEXT_PREVIEW_LIMIT) {
            return sanitized;
        }
        return sanitized.substring(0, TEXT_PREVIEW_LIMIT - 3) + "...";
    }

    private static String csv(String value) {
        return value.replace('"', '\'');
    }

    private static double reductionPct(int base, int reduced) {
        if (base == 0) {
            return 0.0D;
        }
        return ((double) (base - reduced) * 100.0D) / (double) base;
    }

    private static double componentReductionPct(QueryResult result) {
        return reductionPct(
                result.normalized().snapshot().finalComponents(),
                result.canonical().snapshot().finalComponents());
    }

    private static double expandedReductionPct(QueryResult result) {
        return reductionPct(
                result.normalized().snapshot().expandedComponents(),
                result.canonical().snapshot().expandedComponents());
    }

    private record QuerySetSpec(String name, Path queriesPath, Path metadataPath) {
    }

    private record QueryMetadata(
            Map<String, QueryTag> byText,
            List<QueryTag> byOrdinal,
            boolean requiresOrdinalAlignment) {
        private static QueryMetadata empty() {
            return new QueryMetadata(Map.of(), List.of(), false);
        }

        private QueryTag tagFor(String queryText, int ordinal) {
            QueryTag direct = byText.get(queryText);
            if (direct != null) {
                return direct;
            }
            int index = ordinal - 1;
            if (index >= 0 && index < byOrdinal.size()) {
                return byOrdinal.get(index);
            }
            return null;
        }

        private void validateAlignedRowCount(Path metadataPath, int queryCount) {
            if (requiresOrdinalAlignment && queryCount != byOrdinal.size()) {
                throw new IllegalStateException(
                        "Query/metadata size mismatch for " + metadataPath + ": queries=" + queryCount
                                + " metadata_rows=" + byOrdinal.size());
            }
        }
    }

    private record QueryTag(String queryId, String category, String family, String template) {
        private String categoryOrTemplate() {
            return firstNonEmpty(category, template, "uncategorized");
        }
    }

    private record QuerySpec(String setName, int ordinal, String queryId, String category, String family, String text) {
    }

    private enum IdentityMode {
        NORMALIZED,
        CANONICAL_CORE
    }

    private record StructuralSnapshot(
            int registerAttempts,
            int expandedComponents,
            int finalComponents,
            long finalChecksum) {
    }

    private record EnumerationResult(
            long normalizeNanos,
            long identityNanos,
            int registerAttempts,
            int expandedComponents,
            int finalComponents,
            long finalChecksum) {
    }

    private record ModeStats(
            double wallMs,
            double normalizeMs,
            double identityMs,
            StructuralSnapshot snapshot) {
    }

    private record QueryResult(QuerySpec query, int k, int edges, ModeStats normalized, ModeStats canonical) {
    }

    private static final class ModeAccumulator {
        private long wallNanos;
        private long normalizeNanos;
        private long identityNanos;
        private StructuralSnapshot snapshot;

        private void add(
                long measuredWallNanos,
                long measuredNormalizeNanos,
                long measuredIdentityNanos,
                StructuralSnapshot measuredSnapshot) {
            wallNanos += measuredWallNanos;
            normalizeNanos += measuredNormalizeNanos;
            identityNanos += measuredIdentityNanos;
            snapshot = measuredSnapshot;
        }

        private ModeStats finish(int measuredRounds) {
            return new ModeStats(
                    nanosToMillis(wallNanos) / measuredRounds,
                    nanosToMillis(normalizeNanos) / measuredRounds,
                    nanosToMillis(identityNanos) / measuredRounds,
                    snapshot);
        }
    }

    private static final class CategoryAccumulator {
        private int queryCount;
        private int canonicalFasterQueries;
        private double wallRatioSum;
        private double attemptReductionPctSum;
        private double expandedReductionPctSum;
        private double componentReductionPctSum;
        private double maxComponentReductionPct;
        private String maxComponentReductionQueryId;

        private void add(QueryResult result) {
            queryCount++;
            double wallRatio = result.canonical().wallMs() / result.normalized().wallMs();
            if (wallRatio < 1.0D) {
                canonicalFasterQueries++;
            }
            wallRatioSum += wallRatio;
            attemptReductionPctSum += reductionPct(
                    result.normalized().snapshot().registerAttempts(),
                    result.canonical().snapshot().registerAttempts());
            expandedReductionPctSum += expandedReductionPct(result);
            double componentReductionPct = componentReductionPct(result);
            componentReductionPctSum += componentReductionPct;
            if (componentReductionPct > maxComponentReductionPct) {
                maxComponentReductionPct = componentReductionPct;
                maxComponentReductionQueryId = result.query().queryId();
            }
        }

        private double avgWallRatio() {
            return wallRatioSum / queryCount;
        }

        private double avgAttemptReductionPct() {
            return attemptReductionPctSum / queryCount;
        }

        private double avgExpandedReductionPct() {
            return expandedReductionPctSum / queryCount;
        }

        private double avgComponentReductionPct() {
            return componentReductionPctSum / queryCount;
        }
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0D;
    }

    private static final class Enumerator {
        private final int maxCoreDiam;
        private final IdentityMode identityMode;
        private final List<AtomCQ> edges;
        private final int atomCount;
        private int nextId;
        private int registerAttempts;
        private int expandedComponents;
        private long normalizeNanos;
        private long identityNanos;

        private final Map<ComponentKey, Component> bestByKey = new HashMap<>();
        private final Map<VarCQ, List<Component>> bySource = new HashMap<>();
        private final Map<VarCQ, List<Component>> byTarget = new HashMap<>();
        private final Map<EndpointPair, List<Component>> byEndpoints = new HashMap<>();

        private Enumerator(ConjunctiveQuery query, int maxCoreDiam, IdentityMode identityMode) {
            this.maxCoreDiam = maxCoreDiam;
            this.identityMode = identityMode;
            this.edges = query.edges();
            this.atomCount = edges.size();
        }

        private EnumerationResult enumerate() {
            Queue<Component> worklist = new ArrayDeque<>();
            for (int i = 0; i < edges.size(); i++) {
                AtomCQ edge = edges.get(i);
                BitSet owned = new BitSet(atomCount);
                owned.set(i);
                BitSet inverseEmpty = new BitSet(atomCount);
                addAtomicComponent(worklist, edge.getSource(), edge.getTarget(), edge.getLabel(), owned, inverseEmpty);
                if (!edge.getSource().equals(edge.getTarget())) {
                    BitSet inverseSingle = new BitSet(atomCount);
                    inverseSingle.set(i);
                    addAtomicComponent(
                            worklist,
                            edge.getTarget(),
                            edge.getSource(),
                            edge.getLabel().getInverse(),
                            owned,
                            inverseSingle);
                }
            }

            while (!worklist.isEmpty()) {
                Component left = worklist.poll();
                if (left == null || !isCurrent(left)) {
                    continue;
                }
                expandedComponents++;

                List<Component> rightCandidates = bySource.getOrDefault(left.t(), List.of());
                List<Component> parentCandidates = byTarget.getOrDefault(left.s(), List.of());
                int leftId = left.id();
                for (int pass = 0; pass < 2; pass++) {
                    boolean parentFirst = pass == 1;
                    List<Component> candidates = parentFirst ? parentCandidates : rightCandidates;
                    for (int i = 0, size = candidates.size(); i < size; i++) {
                        Component candidate = candidates.get(i);
                        if (!isCurrent(candidate)) {
                            continue;
                        }
                        if (parentFirst ? candidate.id() > leftId : candidate.id() >= leftId) {
                            continue;
                        }

                        Component first = parentFirst ? candidate : left;
                        Component second = parentFirst ? left : candidate;
                        if (first.maskUnsafe().intersects(second.maskUnsafe())) {
                            continue;
                        }
                        int newCore = first.diameter() + second.diameter();
                        if (newCore > maxCoreDiam) {
                            continue;
                        }
                        CPQ cpq = CPQ.concat(List.of(first.cpq(), second.cpq()));
                        CPQ effective = first.s().equals(second.t()) ? CPQ.intersect(cpq, CPQ.id()) : cpq;
                        registerComponent(
                                worklist,
                                first.s(),
                                second.t(),
                                union(first.maskUnsafe(), second.maskUnsafe()),
                                union(first.inverseAtoms(), second.inverseAtoms()),
                                newCore,
                                effective);
                    }
                }

                EndpointPair endpoints = new EndpointPair(left.s(), left.t());
                List<Component> parallelCandidates = byEndpoints.getOrDefault(endpoints, List.of());
                for (int i = 0, size = parallelCandidates.size(); i < size; i++) {
                    Component other = parallelCandidates.get(i);
                    if (other.id() >= left.id() || !isCurrent(other)) {
                        continue;
                    }
                    if (left.maskUnsafe().intersects(other.maskUnsafe())) {
                        continue;
                    }
                    int newCore = Math.max(left.diameter(), other.diameter());
                    if (newCore > maxCoreDiam) {
                        continue;
                    }
                    CPQ cpq = CPQ.intersect(List.of(left.cpq(), other.cpq()));
                    registerComponent(
                            worklist,
                            left.s(),
                            left.t(),
                            union(left.maskUnsafe(), other.maskUnsafe()),
                            union(left.inverseAtoms(), other.inverseAtoms()),
                            newCore,
                            cpq);
                }
            }

            return new EnumerationResult(
                    normalizeNanos,
                    identityNanos,
                    registerAttempts,
                    expandedComponents,
                    bestByKey.size(),
                    finalChecksum());
        }

        private void addAtomicComponent(
                Queue<Component> worklist,
                VarCQ s,
                VarCQ t,
                Predicate label,
                BitSet ownedAtoms,
                BitSet inverseAtoms) {
            CPQ cpq = CPQ.label(label);
            if (s.equals(t)) {
                cpq = CPQ.intersect(cpq, CPQ.id());
            }
            registerComponent(worklist, s, t, ownedAtoms, inverseAtoms, 1, cpq);
        }

        private void registerComponent(
                Queue<Component> worklist,
                VarCQ s,
                VarCQ t,
                BitSet ownedAtoms,
                BitSet inverseAtoms,
                int coreDiam,
                CPQ cpq) {
            registerAttempts++;

            long normalizeStart = System.nanoTime();
            CpqDeduplication.NormalizedCpq normalized = CpqDeduplication.normalizeCpq(cpq);
            normalizeNanos += System.nanoTime() - normalizeStart;

            long identityStart = System.nanoTime();
            String identity = identity(normalized);
            identityNanos += System.nanoTime() - identityStart;

            registerIfBetter(
                    new Component(
                            s,
                            t,
                            coreDiam,
                            ownedAtoms,
                            inverseAtoms,
                            normalized.cpq(),
                            identity,
                            nextId++,
                            normalized.size()),
                    worklist);
        }

        private String identity(CpqDeduplication.NormalizedCpq normalized) {
            return switch (identityMode) {
                case NORMALIZED -> normalized.normalized();
                case CANONICAL_CORE -> CanonForm.computeCanon(normalized.cpq(), false).toBase64Canon();
            };
        }

        private void registerIfBetter(Component component, Queue<Component> worklist) {
            ComponentKey key = key(component);
            Component existing = bestByKey.get(key);
            if (existing != null) {
                if (component.diameter() > existing.diameter()) {
                    return;
                }
                if (component.diameter() == existing.diameter() && component.size() >= existing.size()) {
                    return;
                }
            }

            bestByKey.put(key, component);
            worklist.add(component);
            bySource.computeIfAbsent(component.s(), ignored -> new ArrayList<>()).add(component);
            byTarget.computeIfAbsent(component.t(), ignored -> new ArrayList<>()).add(component);
            byEndpoints.computeIfAbsent(new EndpointPair(component.s(), component.t()), ignored -> new ArrayList<>())
                    .add(component);
        }

        private boolean isCurrent(Component component) {
            return bestByKey.get(key(component)) == component;
        }

        private long finalChecksum() {
            long checksum = 0L;
            for (Component component : bestByKey.values()) {
                checksum += 31L * component.diameter();
                checksum += 17L * component.size();
                checksum += 13L * component.maskUnsafe().hashCode();
                checksum += 7L * component.inverseAtoms().hashCode();
                checksum += component.normalized().hashCode();
            }
            return checksum;
        }

        private static BitSet union(BitSet left, BitSet right) {
            BitSet out = (BitSet) left.clone();
            out.or(right);
            return out;
        }

        private static ComponentKey key(Component component) {
            return new ComponentKey(
                    component.s(),
                    component.t(),
                    component.maskUnsafe(),
                    component.inverseAtoms(),
                    component.normalized());
        }

        private record EndpointPair(VarCQ s, VarCQ t) {
        }

        private record ComponentKey(VarCQ s, VarCQ t, BitSet ownedAtoms, BitSet inverseAtoms, String identity) {
            private ComponentKey {
                Objects.requireNonNull(s, "s");
                Objects.requireNonNull(t, "t");
                Objects.requireNonNull(ownedAtoms, "ownedAtoms");
                Objects.requireNonNull(inverseAtoms, "inverseAtoms");
                Objects.requireNonNull(identity, "identity");
            }
        }
    }
}
