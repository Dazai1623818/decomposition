package evaluator.bench;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import evaluator.bench.BenchTypes.CandidateSelection;
import evaluator.bench.BenchTypes.DecompositionCandidate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.ExecutablePlan;
import evaluator.evaluation.Relation;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Replays archived compare cases on a standalone path and emits exact
 * intermediate-result progression for the archived join order. Unlike the main
 * benchmark pipeline, this tool exists purely for mechanism analysis.
 */
public final class MechanismProbeMain {
    private static final int DEFAULT_COVER_LIMIT = 10;
    private static final int DEFAULT_K = 2;
    private static final int DEFAULT_DECOMPOSITION_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_EXECUTION_TIMEOUT_MS = 10_000;

    private MechanismProbeMain() {
    }

    public static void main(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        Files.createDirectories(parsed.outputDir());
        List<ProbeCase> cases = loadCases(parsed.casesFile());
        Map<Path, ProbeContext> contexts = new HashMap<>();

        Path summaryPath = parsed.outputDir().resolve("probe_summary.tsv");
        Path componentPath = parsed.outputDir().resolve("probe_components.tsv");
        Path intermediatePath = parsed.outputDir().resolve("probe_intermediate_steps.tsv");
        int okCases = 0;
        int timeoutCases = 0;
        int planningTimeoutCases = 0;
        int missingCases = 0;
        int errorCases = 0;

        try (
                PrintWriter summaryOut = newWriter(summaryPath);
                PrintWriter componentOut = newWriter(componentPath);
                PrintWriter intermediateOut = newWriter(intermediatePath)
        ) {
            writeSummaryHeader(summaryOut);
            writeComponentHeader(componentOut);
            writeIntermediateHeader(intermediateOut);

            for (ProbeCase probeCase : cases) {
                System.out.println("probe_case_start=" + probeCase.caseId());
                ProbeContext context = contexts.computeIfAbsent(probeCase.indexPath(), MechanismProbeMain::loadContext);
                CaseReport report = emitCase(
                        summaryOut,
                        componentOut,
                        intermediateOut,
                        context,
                        probeCase,
                        parsed.coverLimit(),
                        parsed.k(),
                        parsed.decompositionTimeoutMs(),
                        parsed.executionTimeoutMs());
                switch (report.status()) {
                    case "OK" -> okCases++;
                    case "EXEC_TIMEOUT" -> timeoutCases++;
                    case "PLANNING_TIMEOUT" -> planningTimeoutCases++;
                    case "NO_CANDIDATE" -> missingCases++;
                    default -> errorCases++;
                }
                System.out.println("probe_case_done=" + probeCase.caseId() + " status=" + report.status());
            }
        }

        System.out.println("mechanism_probe_output_dir=" + parsed.outputDir().toAbsolutePath());
        System.out.println("probe_summary_file=" + summaryPath.getFileName());
        System.out.println("probe_components_file=" + componentPath.getFileName());
        System.out.println("probe_intermediate_file=" + intermediatePath.getFileName());
        System.out.println(String.format(
                Locale.ROOT,
                "probe_case_counts total=%d ok=%d exec_timeout=%d planning_timeout=%d no_candidate=%d error=%d",
                cases.size(),
                okCases,
                timeoutCases,
                planningTimeoutCases,
                missingCases,
                errorCases));
    }

    private static PrintWriter newWriter(Path path) throws Exception {
        return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8));
    }

    private static ProbeContext loadContext(Path indexPath) {
        try {
            LeanNativeIndex index = LeanNativeIndex.load(indexPath);
            EngineConfig config = EngineConfig.defaults();
            BenchEngine engine = new BenchEngine(index, config);
            return new ProbeContext(index, engine, config);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load probe context for " + indexPath, ex);
        }
    }

    private static CaseReport emitCase(
            PrintWriter summaryOut,
            PrintWriter componentOut,
            PrintWriter intermediateOut,
            ProbeContext context,
            ProbeCase probeCase,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int executionTimeoutMs) {
        long startedNanos = System.nanoTime();
        long parseStartNanos = System.nanoTime();

        final ConjunctiveQuery cq;
        try {
            cq = context.engine().parseCQ(probeCase.queryText());
        } catch (RuntimeException ex) {
            long parseNanos = System.nanoTime() - parseStartNanos;
            emitSummary(summaryOut, SummaryRow.error(
                    probeCase,
                    "ERROR",
                    parseNanos,
                    0L,
                    0L,
                    0L,
                    System.nanoTime() - startedNanos,
                    summarizeError(ex)));
            return new CaseReport("ERROR");
        }
        long parseNanos = System.nanoTime() - parseStartNanos;

        long planningStartNanos = System.nanoTime();
        CandidateSelection selection;
        try {
            selection = context.engine().selectBestCandidatesWithTimeoutInfo(
                    cq,
                    coverLimit,
                    k,
                    decompositionTimeoutMs,
                    executionTimeoutMs);
        } catch (RuntimeException ex) {
            emitSummary(summaryOut, SummaryRow.error(
                    probeCase,
                    "ERROR",
                    parseNanos,
                    System.nanoTime() - planningStartNanos,
                    0L,
                    0L,
                    System.nanoTime() - startedNanos,
                    summarizeError(ex)));
            return new CaseReport("ERROR");
        }
        long planningNanos = System.nanoTime() - planningStartNanos;

        DecompositionCandidate candidate = findCandidate(selection.candidates(), probeCase.method());
        long candidatePlanningNanos = candidate == null
                ? selection.decompositionNanosByMethod().getOrDefault(probeCase.method(), planningNanos)
                : candidate.decomposeNanos();
        if (candidate == null) {
            String status = selection.timedOutMethods().contains(probeCase.method())
                    ? "PLANNING_TIMEOUT"
                    : "NO_CANDIDATE";
            emitSummary(summaryOut, SummaryRow.noCandidate(
                    probeCase,
                    status,
                    parseNanos,
                    candidatePlanningNanos,
                    System.nanoTime() - startedNanos));
            return new CaseReport(status);
        }

        long deadlineNanos = Deadline.afterMillis(executionTimeoutMs);
        long compileStartNanos = System.nanoTime();
        final ExecutablePlan executable;
        try {
            executable = ExecutablePlan.compile(candidate.decomposition(), context.index(), deadlineNanos);
        } catch (Deadline.Exceeded | CancellationException ex) {
            emitSummary(summaryOut, SummaryRow.execTimeout(
                    probeCase,
                    candidate,
                    parseNanos,
                    candidatePlanningNanos,
                    System.nanoTime() - compileStartNanos,
                    0L,
                    System.nanoTime() - startedNanos,
                    "compile",
                    null));
            return new CaseReport("EXEC_TIMEOUT");
        } catch (RuntimeException ex) {
            emitSummary(summaryOut, SummaryRow.error(
                    probeCase,
                    "ERROR",
                    parseNanos,
                    candidatePlanningNanos,
                    System.nanoTime() - compileStartNanos,
                    0L,
                    System.nanoTime() - startedNanos,
                    summarizeError(ex)));
            return new CaseReport("ERROR");
        }
        long compileNanos = System.nanoTime() - compileStartNanos;

        long traceStartNanos = System.nanoTime();
        IntermediateResultTracer.Trace trace;
        try {
            trace = IntermediateResultTracer.trace(
                    executable,
                    probeCase.variableOrder(),
                    context.config().joinSafeDistinctFastPath(),
                    deadlineNanos);
        } catch (RuntimeException ex) {
            emitSummary(summaryOut, SummaryRow.error(
                    probeCase,
                    "ERROR",
                    parseNanos,
                    candidatePlanningNanos,
                    compileNanos,
                    System.nanoTime() - traceStartNanos,
                    System.nanoTime() - startedNanos,
                    summarizeError(ex)));
            return new CaseReport("ERROR");
        }
        long traceNanos = System.nanoTime() - traceStartNanos;

        String caseStatus = trace.status().name();
        emitComponents(componentOut, probeCase, candidate, executable, caseStatus);
        emitIntermediateSteps(intermediateOut, probeCase, candidate, trace);
        emitSummary(summaryOut, SummaryRow.complete(
                probeCase,
                candidate,
                trace,
                parseNanos,
                candidatePlanningNanos,
                compileNanos,
                traceNanos,
                System.nanoTime() - startedNanos));
        return new CaseReport(caseStatus);
    }

    private static void emitComponents(
            PrintWriter componentOut,
            ProbeCase probeCase,
            DecompositionCandidate candidate,
            ExecutablePlan executable,
            String caseStatus) {
        List<Plan.Component> components = executable.components();
        List<Relation> relations = executable.relations();
        for (int i = 0; i < components.size(); i++) {
            Plan.Component component = components.get(i);
            Relation relation = relations.get(i);
            long tupleCount = executable.componentCounts().get(i);
            long sourceNvd = relation.ndv(component.sourceVarName());
            long targetNdv = component.isUnary() ? sourceNvd : relation.ndv(component.targetVarName());
            componentOut.println(String.join(
                    "\t",
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    candidate.method().id(),
                    Integer.toString(candidate.ordinal()),
                    caseStatus,
                    Integer.toString(i + 1),
                    component.sourceVarName(),
                    component.targetVarName(),
                    Integer.toString(component.diameter()),
                    component.mask().toString(),
                    component.cpq().toString(),
                    component.normalized(),
                    Long.toString(tupleCount),
                    Long.toString(sourceNvd),
                    Long.toString(targetNdv)));
        }
        componentOut.flush();
    }

    private static void emitIntermediateSteps(
            PrintWriter intermediateOut,
            ProbeCase probeCase,
            DecompositionCandidate candidate,
            IntermediateResultTracer.Trace trace) {
        String answerCount = trace.answerCount() < 0L ? "" : Long.toString(trace.answerCount());
        String timeoutStage = nullToEmpty(trace.timeoutStageLabel());
        String timeoutVariable = nullToEmpty(trace.timeoutVariable());
        for (IntermediateResultTracer.Stage stage : trace.stages()) {
            intermediateOut.println(String.join(
                    "\t",
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    candidate.method().id(),
                    Integer.toString(candidate.ordinal()),
                    trace.status().name(),
                    Integer.toString(stage.stageIndex()),
                    stage.stageLabel(),
                    nullToEmpty(stage.variable()),
                    joinVariables(stage.prefixOrder()),
                    joinVariables(trace.fullOrder()),
                    joinVariables(trace.projectedOrder()),
                    Long.toString(stage.count()),
                    Long.toString(stage.previousCount()),
                    formatRatio(stage.retainedVsPrevious()),
                    formatRatio(stage.retainedVsMaterialized()),
                    Long.toString(trace.sumComponentTuples()),
                    Long.toString(trace.maxComponentTuples()),
                    answerCount,
                    timeoutStage,
                    timeoutVariable));
        }
        intermediateOut.flush();
    }

    private static DecompositionCandidate findCandidate(
            List<DecompositionCandidate> candidates,
            DecompositionMethod method) {
        for (DecompositionCandidate candidate : candidates) {
            if (candidate.method() == method) {
                return candidate;
            }
        }
        return null;
    }

    private static String summarizeError(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        if (message == null || message.isBlank()) {
            return cause.getClass().getSimpleName();
        }
        return cause.getClass().getSimpleName() + ": " + message;
    }

    private static void emitSummary(PrintWriter summaryOut, SummaryRow row) {
        summaryOut.println(String.join(
                "\t",
                row.caseId(),
                row.caseLabel(),
                row.dataset(),
                row.template(),
                row.method(),
                row.ordinal(),
                row.status(),
                row.components(),
                row.maxDiameter(),
                row.projectedVars(),
                row.variableOrder(),
                row.sumComponentTuples(),
                row.maxComponentTuples(),
                row.answerCount(),
                row.stagesEmitted(),
                row.timeoutStage(),
                row.timeoutVariable(),
                formatMillis(row.parseNanos()),
                formatMillis(row.planningNanos()),
                formatMillis(row.compileNanos()),
                formatMillis(row.traceNanos()),
                formatMillis(row.elapsedNanos()),
                row.errorMessage(),
                row.queryText()));
        summaryOut.flush();
    }

    private static String formatMillis(long nanos) {
        return String.format(Locale.ROOT, "%.3f", nanos / 1_000_000.0d);
    }

    private static String formatRatio(double value) {
        return Double.isNaN(value) ? "" : String.format(Locale.ROOT, "%.6f", value);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String joinVariables(List<String> variables) {
        return String.join("|", variables);
    }

    private static List<ProbeCase> loadCases(Path path) throws Exception {
        List<ProbeCase> cases = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                return List.of();
            }
            Map<String, Integer> indexByColumn = headerIndex(header);
            for (String line; (line = reader.readLine()) != null;) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                cases.add(new ProbeCase(
                        field(fields, indexByColumn, "case_id"),
                        field(fields, indexByColumn, "case_label"),
                        field(fields, indexByColumn, "dataset"),
                        field(fields, indexByColumn, "template"),
                        DecompositionMethod.fromToken(field(fields, indexByColumn, "method")),
                        Path.of(field(fields, indexByColumn, "index_path")),
                        field(fields, indexByColumn, "query_text"),
                        parseVariableOrder(field(fields, indexByColumn, "variable_order"))));
            }
        }
        return List.copyOf(cases);
    }

    private static List<String> parseVariableOrder(String token) {
        if (token == null || token.isBlank()) {
            return List.of();
        }
        String trimmed = token.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        if (trimmed.isBlank()) {
            return List.of();
        }
        String[] parts = trimmed.split(",");
        List<String> order = new ArrayList<>(parts.length);
        for (String part : parts) {
            order.add(part.trim());
        }
        return List.copyOf(order);
    }

    private static Map<String, Integer> headerIndex(String header) {
        String[] columns = header.split("\t", -1);
        Map<String, Integer> indexByColumn = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            indexByColumn.put(columns[i].trim(), i);
        }
        return Map.copyOf(indexByColumn);
    }

    private static String field(String[] fields, Map<String, Integer> indexByColumn, String name) {
        Integer index = indexByColumn.get(name);
        if (index == null || index < 0 || index >= fields.length) {
            throw new IllegalArgumentException("Missing required TSV column: " + name);
        }
        return fields[index];
    }

    private static void writeSummaryHeader(PrintWriter out) {
        out.println(String.join(
                "\t",
                "case_id",
                "case_label",
                "dataset",
                "template",
                "method",
                "ordinal",
                "status",
                "components",
                "max_diameter",
                "projected_vars",
                "variable_order",
                "sum_component_tuples",
                "max_component_tuples",
                "answer_count",
                "stages_emitted",
                "timeout_stage",
                "timeout_variable",
                "parse_ms",
                "planning_ms",
                "compile_ms",
                "trace_ms",
                "elapsed_ms",
                "error",
                "query_text"));
    }

    private static void writeComponentHeader(PrintWriter out) {
        out.println(String.join(
                "\t",
                "case_id",
                "case_label",
                "dataset",
                "template",
                "method",
                "ordinal",
                "case_status",
                "component_index",
                "source_var",
                "target_var",
                "diameter",
                "mask",
                "cpq",
                "normalized",
                "tuple_count",
                "source_ndv",
                "target_ndv"));
    }

    private static void writeIntermediateHeader(PrintWriter out) {
        out.println(String.join(
                "\t",
                "case_id",
                "case_label",
                "dataset",
                "template",
                "method",
                "ordinal",
                "case_status",
                "stage_index",
                "stage_label",
                "variable",
                "prefix_vars",
                "full_order",
                "projected_order",
                "count",
                "previous_count",
                "retained_vs_previous",
                "retained_vs_materialized",
                "sum_component_tuples",
                "max_component_tuples",
                "answer_count",
                "timeout_stage",
                "timeout_variable"));
    }

    private record ProbeContext(
            LeanNativeIndex index,
            BenchEngine engine,
            EngineConfig config) {
    }

    private record CaseReport(String status) {
    }

    private record SummaryRow(
            String caseId,
            String caseLabel,
            String dataset,
            String template,
            String method,
            String ordinal,
            String status,
            String components,
            String maxDiameter,
            String projectedVars,
            String variableOrder,
            String sumComponentTuples,
            String maxComponentTuples,
            String answerCount,
            String stagesEmitted,
            String timeoutStage,
            String timeoutVariable,
            long parseNanos,
            long planningNanos,
            long compileNanos,
            long traceNanos,
            long elapsedNanos,
            String errorMessage,
            String queryText) {
        private static SummaryRow complete(
                ProbeCase probeCase,
                DecompositionCandidate candidate,
                IntermediateResultTracer.Trace trace,
                long parseNanos,
                long planningNanos,
                long compileNanos,
                long traceNanos,
                long elapsedNanos) {
            return new SummaryRow(
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    candidate.method().id(),
                    Integer.toString(candidate.ordinal()),
                    trace.status().name(),
                    Integer.toString(candidate.decomposition().size()),
                    Integer.toString(candidate.decomposition().maxDiameter()),
                    joinVariables(candidate.decomposition().projectedVariableNames()),
                    joinVariables(trace.fullOrder()),
                    Long.toString(trace.sumComponentTuples()),
                    Long.toString(trace.maxComponentTuples()),
                    trace.answerCount() < 0L ? "" : Long.toString(trace.answerCount()),
                    Integer.toString(trace.stages().size()),
                    nullToEmpty(trace.timeoutStageLabel()),
                    nullToEmpty(trace.timeoutVariable()),
                    parseNanos,
                    planningNanos,
                    compileNanos,
                    traceNanos,
                    elapsedNanos,
                    "",
                    probeCase.queryText());
        }

        private static SummaryRow execTimeout(
                ProbeCase probeCase,
                DecompositionCandidate candidate,
                long parseNanos,
                long planningNanos,
                long compileNanos,
                long traceNanos,
                long elapsedNanos,
                String timeoutStage,
                String timeoutVariable) {
            return new SummaryRow(
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    candidate.method().id(),
                    Integer.toString(candidate.ordinal()),
                    "EXEC_TIMEOUT",
                    Integer.toString(candidate.decomposition().size()),
                    Integer.toString(candidate.decomposition().maxDiameter()),
                    joinVariables(candidate.decomposition().projectedVariableNames()),
                    joinVariables(probeCase.variableOrder()),
                    "",
                    "",
                    "",
                    "0",
                    timeoutStage,
                    nullToEmpty(timeoutVariable),
                    parseNanos,
                    planningNanos,
                    compileNanos,
                    traceNanos,
                    elapsedNanos,
                    "",
                    probeCase.queryText());
        }

        private static SummaryRow noCandidate(
                ProbeCase probeCase,
                String status,
                long parseNanos,
                long planningNanos,
                long elapsedNanos) {
            return new SummaryRow(
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    probeCase.method().id(),
                    "",
                    status,
                    "",
                    "",
                    "",
                    joinVariables(probeCase.variableOrder()),
                    "",
                    "",
                    "",
                    "0",
                    "",
                    "",
                    parseNanos,
                    planningNanos,
                    0L,
                    0L,
                    elapsedNanos,
                    "",
                    probeCase.queryText());
        }

        private static SummaryRow error(
                ProbeCase probeCase,
                String status,
                long parseNanos,
                long planningNanos,
                long compileNanos,
                long traceNanos,
                long elapsedNanos,
                String errorMessage) {
            return new SummaryRow(
                    probeCase.caseId(),
                    probeCase.caseLabel(),
                    probeCase.dataset(),
                    probeCase.template(),
                    probeCase.method().id(),
                    "",
                    status,
                    "",
                    "",
                    "",
                    joinVariables(probeCase.variableOrder()),
                    "",
                    "",
                    "",
                    "0",
                    "",
                    "",
                    parseNanos,
                    planningNanos,
                    compileNanos,
                    traceNanos,
                    elapsedNanos,
                    errorMessage == null ? "" : errorMessage,
                    probeCase.queryText());
        }
    }

    /**
     * Probe-only index wrapper that skips the expensive synopsis construction
     * in {@code NativeCpqIndex}. The probe only needs query, cost, and support
     * checks.
     */
    private static final class LeanNativeIndex implements CpqIndex {
        static {
            try {
                Main.loadNatives();
            } catch (Exception ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }

        private final Index index;
        private final int k;
        private final int intersections;

        private LeanNativeIndex(Index index) {
            this.index = Objects.requireNonNull(index, "index");
            this.k = index.getK();
            this.intersections = index.getIntersections();
        }

        private static LeanNativeIndex load(Path path) throws Exception {
            try (InputStream in = Files.newInputStream(path)) {
                return new LeanNativeIndex(new Index(in));
            }
        }

        @Override
        public int k() {
            return k;
        }

        @Override
        public int intersections() {
            return intersections;
        }

        @Override
        public boolean isIndexable(dev.roanh.gmark.lang.cpq.CPQ cpq) {
            return cpq.getDiameter() <= k;
        }

        @Override
        public long cost(dev.roanh.gmark.lang.cpq.CPQ cpq) {
            return index.cost(cpq);
        }

        @Override
        public List<Edge> query(dev.roanh.gmark.lang.cpq.CPQ cpq) {
            List<Pair> pairs = index.query(cpq);
            List<Edge> edges = new ArrayList<>(pairs.size());
            for (Pair pair : pairs) {
                edges.add(new Edge(pair.getSource(), pair.getTarget()));
            }
            return edges;
        }

        @Override
        public QueryMatches queryMatches(dev.roanh.gmark.lang.cpq.CPQ cpq) {
            List<Pair> pairs = index.query(cpq);
            return new QueryMatches() {
                @Override
                public int size() {
                    return pairs.size();
                }

                @Override
                public void forEach(IntPairConsumer consumer) {
                    for (Pair pair : pairs) {
                        consumer.accept(pair.getSource(), pair.getTarget());
                    }
                }
            };
        }
    }

    private record ProbeCase(
            String caseId,
            String caseLabel,
            String dataset,
            String template,
            DecompositionMethod method,
            Path indexPath,
            String queryText,
            List<String> variableOrder) {
    }

    private record ParsedArgs(
            Path casesFile,
            Path outputDir,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int executionTimeoutMs) {
        private static ParsedArgs parse(String[] args) {
            Path casesFile = null;
            Path outputDir = null;
            int coverLimit = DEFAULT_COVER_LIMIT;
            int k = DEFAULT_K;
            int decompositionTimeoutMs = DEFAULT_DECOMPOSITION_TIMEOUT_MS;
            int executionTimeoutMs = DEFAULT_EXECUTION_TIMEOUT_MS;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--cases-file" -> casesFile = Path.of(requireValue(args, ++i, "--cases-file"));
                    case "--output-dir" -> outputDir = Path.of(requireValue(args, ++i, "--output-dir"));
                    case "--cover-limit" -> coverLimit = Integer.parseInt(requireValue(args, ++i, "--cover-limit"));
                    case "--k" -> k = Integer.parseInt(requireValue(args, ++i, "--k"));
                    case "--decomposition-timeout-ms" ->
                        decompositionTimeoutMs = Integer.parseInt(requireValue(args, ++i, "--decomposition-timeout-ms"));
                    case "--execution-timeout-ms" ->
                        executionTimeoutMs = Integer.parseInt(requireValue(args, ++i, "--execution-timeout-ms"));
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }

            if (casesFile == null) {
                throw new IllegalArgumentException("Missing required --cases-file");
            }
            if (outputDir == null) {
                throw new IllegalArgumentException("Missing required --output-dir");
            }
            if (coverLimit < 0) {
                throw new IllegalArgumentException("--cover-limit must be >= 0");
            }
            if (k <= 0) {
                throw new IllegalArgumentException("--k must be > 0");
            }
            if (decompositionTimeoutMs < 0) {
                throw new IllegalArgumentException("--decomposition-timeout-ms must be >= 0");
            }
            if (executionTimeoutMs < 0) {
                throw new IllegalArgumentException("--execution-timeout-ms must be >= 0");
            }
            return new ParsedArgs(
                    casesFile,
                    outputDir,
                    coverLimit,
                    k,
                    decompositionTimeoutMs,
                    executionTimeoutMs);
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }
    }
}
