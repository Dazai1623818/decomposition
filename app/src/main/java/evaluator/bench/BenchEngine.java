package evaluator.bench;

import static evaluator.bench.BenchTypes.*;

import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan.Component;
import evaluator.evaluation.Planner;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.WanderJoinEstimator;
import evaluator.cpq.Plan;
import evaluator.evaluation.Relation;
import evaluator.evaluation.ExecutablePlan;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class BenchEngine {
    private static final int COMPARE_FILE_PROGRESS_EVERY = 25;

    private final CpqIndex index;
    private final Planner planner;
    private final EngineConfig config;
    private final JoinOrderSelector joinOrderSelector;

    BenchEngine(CpqIndex index) {
        this(index, EngineConfig.defaults());
    }

    BenchEngine(CpqIndex index, EngineConfig config) {
        this.index = Objects.requireNonNull(index, "index");
        this.config = Objects.requireNonNull(config, "config");
        if (index.k() < 1) {
            throw new IllegalArgumentException("index k must be >= 1");
        }
        this.planner = new Planner(index, config.estimationSeed());
        this.joinOrderSelector = new JoinOrderSelector();
    }

    ConjunctiveQuery parseCQ(String text) {
        return ConjunctiveQuery.parse(text);
    }

    /**
     * Runs the eval-file workflow on all non-comment query lines.
     */
    public BenchTypes.EvalFileReport evalFile(BenchTypes.EvalFileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        EvaluationMode mode = Objects.requireNonNull(spec.evaluationMode(), "evaluationMode");
        int coverLimit = spec.coverLimit();
        int k = spec.kOverride() > 0 ? spec.kOverride() : index.k();
        int decompositionTimeoutMs = Math.max(0, spec.decompositionTimeoutMs());
        int methodTimeoutMs = Math.max(0, spec.methodTimeoutMs());
        BenchTypes.EvalFileProgressSink progressSink = spec.progressSink() == null
                ? BenchTypes.EvalFileProgressSink.NOOP
                : spec.progressSink();
        int queryCount = 0;
        int failures = 0;
        int timeouts = 0;

        try (BufferedReader reader = Files.newBufferedReader(spec.queriesFile(), StandardCharsets.UTF_8)) {
            for (String line; (line = reader.readLine()) != null;) {
                String query = line.trim();
                if (query.isEmpty() || query.startsWith("#")) {
                    continue;
                }
                queryCount++;
                BenchTypes.EvalFileStatus status = BenchTypes.EvalFileStatus.ERROR;
                String methodId = null;
                int components = 0;
                int maxDiameter = 0;
                long answers = -1L;
                long parseNanos = 0L;
                long decomposeNanos = 0L;
                long queryNanos = 0L;
                long mappingNanos = 0L;
                long estimateNanos = 0L;
                long joinNanos = 0L;
                String errorMessage = null;
                long wallStart = System.nanoTime();
                try {
                    long parseStart = System.nanoTime();
                    ConjunctiveQuery cq = parseCQ(query);
                    parseNanos = System.nanoTime() - parseStart;
                    long decomposeStart = System.nanoTime();
                    PreparedCandidateSelection candidates = selectBestPreparedCandidatesWithTimeoutInfo(
                            cq,
                            coverLimit,
                            k,
                            decompositionTimeoutMs,
                            Long.MAX_VALUE);
                    decomposeNanos = System.nanoTime() - decomposeStart;
                    if (candidates.candidates().isEmpty()) {
                        failures++;
                        status = BenchTypes.EvalFileStatus.NO_DECOMPOSITIONS;
                        continue;
                    }
                    PreparedCandidate selected = candidates.candidates().get(0);
                    methodId = selected.candidate().method().id();
                    components = selected.candidate().decomposition().size();
                    maxDiameter = selected.candidate().decomposition().maxDiameter();
                    long deadlineNanos = Deadline.afterMillis(methodTimeoutMs);
                    EvaluationWithStats evaluation;
                    try {
                        evaluation = evaluateWithStats(selected.executable(), mode, deadlineNanos);
                    } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
                        failures++;
                        timeouts++;
                        status = BenchTypes.EvalFileStatus.TIMEOUT;
                        continue;
                    }
                    queryNanos = evaluation.stats().queryNanos();
                    mappingNanos = evaluation.stats().mappingNanos();
                    estimateNanos = evaluation.stats().estimateNanos();
                    joinNanos = evaluation.stats().joinNanos();
                    answers = answerCount(evaluation.result());
                    status = BenchTypes.EvalFileStatus.OK;
                } catch (Exception ex) {
                    failures++;
                    status = BenchTypes.EvalFileStatus.ERROR;
                    errorMessage = summarizeError(ex);
                } finally {
                    progressSink.onQueryResult(new BenchTypes.EvalFileProgress(
                            queryCount,
                            query,
                            methodId,
                            components,
                            maxDiameter,
                            answers,
                            status,
                            parseNanos,
                            decomposeNanos,
                            queryNanos,
                            mappingNanos,
                            estimateNanos,
                            joinNanos,
                            System.nanoTime() - wallStart,
                            errorMessage));
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to evaluate query file: " + spec.queriesFile(), ex);
        }

        return new BenchTypes.EvalFileReport(queryCount, failures, timeouts);
    }

    private static long answerCount(EvaluationResult result) {
        if (result instanceof CountResult count) {
            return count.count();
        }
        if (result instanceof RowResult rows) {
            return rows.rows().size();
        }
        throw new IllegalArgumentException("Unsupported evaluation result type: " + result.getClass().getName());
    }

    private static List<DecompositionCandidate> toBenchCandidates(List<Planner.Candidate> candidates) {
        List<DecompositionCandidate> mapped = new ArrayList<>(candidates.size());
        for (Planner.Candidate candidate : candidates) {
            mapped.add(toBenchCandidate(candidate));
        }
        return List.copyOf(mapped);
    }

    private static DecompositionCandidate toBenchCandidate(Planner.Candidate candidate) {
        return new DecompositionCandidate(
                candidate.method(),
                candidate.ordinal(),
                candidate.plan(),
                candidate.decomposeNanos());
    }

    private PreparedCandidate prepareCandidate(DecompositionCandidate candidate, long deadlineNanos) {
        Objects.requireNonNull(candidate, "candidate");
        return new PreparedCandidate(
                candidate,
                ExecutablePlan.compile(candidate.decomposition(), index, deadlineNanos));
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

    /**
     * Runs the explore workflow and returns the number of selected candidates.
     */
    public BenchTypes.ExploreReport explore(BenchTypes.ExploreSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        ExploreCandidates candidates = prepareExploreCandidates(
                cq,
                spec.coverLimit(),
                spec.k() > 0 ? spec.k() : index.k(),
                Math.max(0, spec.decompositionTimeoutMs()),
                Math.max(0, spec.minComponents()),
                Math.max(0, spec.maxComponents()),
                Math.max(0, spec.candidateLimit()));
        return new BenchTypes.ExploreReport(candidates.status(), candidates.selectedCandidates().size());
    }

    /**
     * Runs the compare workflow and returns compared method/candidate count.
     */
    public BenchTypes.CompareReport compare(BenchTypes.CompareSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        EvaluationMode mode = Objects.requireNonNull(spec.evaluationMode(), "evaluationMode");
        int methodTimeoutMs = Math.max(0, spec.methodTimeoutMs());
        ParsedQuery parsed = new ParsedQuery(cq, 0L, null);
        ComparisonPreparationStatus status = ComparisonPreparationStatus.NO_DECOMPOSITIONS;
        int completed = 0;
        int timeouts = 0;
        for (DecompositionMethod method : compareFileMethods()) {
            MethodOutcome<EvaluationWithStats> outcome = executeMethodRun(
                    parsed,
                    method,
                    spec.coverLimit(),
                    spec.k() > 0 ? spec.k() : index.k(),
                    Math.max(0, spec.decompositionTimeoutMs()),
                    methodTimeoutMs,
                    (candidate, deadlineNanos) -> evaluateWithStats(candidate.executable(), mode, deadlineNanos));
            switch (outcome.type()) {
                case SUCCESS -> {
                    status = ComparisonPreparationStatus.READY;
                    completed++;
                }
                case TIMEOUT -> {
                    status = ComparisonPreparationStatus.READY;
                    timeouts++;
                }
                case MISSING_CANDIDATE -> {
                    if ("DECOMP_TIMEOUT".equals(outcome.status())) {
                        status = ComparisonPreparationStatus.READY;
                    }
                }
                case PARSE_ERROR -> throw new IllegalStateException("compare parses the query before method execution");
            }
        }
        return new BenchTypes.CompareReport(status, completed, timeouts);
    }

    /**
     * Runs file-level compare output workflow.
     */
    public BenchTypes.CompareFileReport compareFile(BenchTypes.CompareFileSpec spec) throws Exception {
        Objects.requireNonNull(spec, "spec");
        List<String> queries = loadQueries(spec.queriesFile());
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.kOverride(),
                spec.coverLimit(),
                spec.decompositionTimeoutMs(),
                spec.methodTimeoutMs(),
                index.k());
        int k = runConfig.k();
        int coverLimit = runConfig.coverLimit();
        int decompositionTimeoutMs = runConfig.decompositionTimeoutMs();
        int methodTimeoutMs = runConfig.methodTimeoutMs();
        List<DecompositionMethod> methods = compareFileMethods();

        PrintWriter compareOut = openCompareWriter(spec.compareLogPath());
        boolean closeCompareOut = spec.compareLogPath() != null;
        try (PrintWriter decompositionOut = openDecompositionWriter(spec.decompositionLogPath())) {
            String command = "compare-file --index " + spec.indexPath()
                    + " --queries-file " + spec.queriesFile()
                    + " --method-timeout-ms " + methodTimeoutMs
                    + " --decomposition-timeout-ms " + decompositionTimeoutMs
                    + " --cover-limit " + coverLimit
                    + " --k " + k
                    + " --seed " + spec.seed();
            printCompareFileHeader(
                    compareOut,
                    spec,
                    queries.size(),
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    coverLimit,
                    k,
                    command,
                    config.estimationSeed());
            if (decompositionOut != null) {
                decompositionOut.println("# " + command);
            }

            long startedNanos = System.nanoTime();
            long methodRows = 0L;
            long okRows = 0L;
            long timeoutRows = 0L;
            long decompTimeoutRows = 0L;
            long noCandidateRows = 0L;
            long errorRows = 0L;
            int queryNumber = 0;

            for (String queryText : queries) {
                queryNumber++;
                List<MethodOutcome<EvaluationWithStats>> outcomes = evaluateQueryAcrossMethods(
                        queryText,
                        methods,
                        coverLimit,
                        k,
                        decompositionTimeoutMs,
                        methodTimeoutMs,
                        (candidate, deadlineNanos) -> evaluateWithStats(
                                candidate.executable(),
                                EvaluationMode.COUNT,
                                deadlineNanos));

                for (MethodOutcome<EvaluationWithStats> outcome : outcomes) {
                    methodRows++;
                    DecompositionCandidate candidate = outcome.candidate();
                    if (candidate != null && decompositionOut != null) {
                        decompositionOut.println(formatDecompositionLine(queryNumber, candidate));
                    }
                    switch (outcome.type()) {
                        case PARSE_ERROR -> {
                            emitCompareFileRow(compareOut, CompareFileRow.parseError(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.wallNanos(),
                                    outcome.errorMessage()));
                            errorRows++;
                        }
                        case MISSING_CANDIDATE -> {
                            emitCompareFileRow(compareOut, CompareFileRow.withoutCandidate(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos(),
                                    outcome.status()));
                            if ("DECOMP_TIMEOUT".equals(outcome.status())) {
                                decompTimeoutRows++;
                            } else {
                                noCandidateRows++;
                            }
                        }
                        case TIMEOUT -> {
                            int edgesCollapsed = candidate == null ? 0 : edgesCollapsed(candidate.decomposition());
                            emitCompareFileRow(compareOut, CompareFileRow.timeout(
                                    queryNumber,
                                    outcome.method(),
                                    candidate,
                                    edgesCollapsed,
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos()));
                            timeoutRows++;
                        }
                        case SUCCESS -> {
                            int edgesCollapsed = edgesCollapsed(candidate.decomposition());
                            emitCompareFileRow(compareOut, CompareFileRow.ok(
                                    queryNumber,
                                    outcome.method(),
                                    candidate,
                                    edgesCollapsed,
                                    outcome.parseNanos(),
                                    outcome.wallNanos(),
                                    outcome.evaluation()));
                            okRows++;
                        }
                    }
                }

                if (queryNumber % COMPARE_FILE_PROGRESS_EVERY == 0) {
                    compareOut.println(String.format(
                            Locale.ROOT,
                            "progress_time=%s queries_completed=%d/%d method_rows=%d approx_pct=%.2f",
                            Instant.now(),
                            queryNumber,
                            queries.size(),
                            methodRows,
                            (queryNumber * 100.0d) / Math.max(1, queries.size())));
                    compareOut.flush();
                    if (decompositionOut != null) {
                        decompositionOut.flush();
                    }
                }
            }

            long elapsedNanos = System.nanoTime() - startedNanos;
            compareOut.println(String.format(
                    Locale.ROOT,
                    "summary query_count=%d method_rows=%d ok=%d timeout=%d decomp_timeout=%d no_candidate=%d error=%d elapsed_ms=%.3f",
                    queries.size(),
                    methodRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    nanosToMillis(elapsedNanos)));
            compareOut.flush();
            if (decompositionOut != null) {
                decompositionOut.flush();
            }

            return new BenchTypes.CompareFileReport(
                    queries.size(),
                    methodRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    elapsedNanos);
        } finally {
            closeOwnedWriter(compareOut, closeCompareOut);
        }
    }

    public BenchTypes.EstimationBenchReport estimationBench(BenchTypes.EstimationBenchSpec spec) throws Exception {
        Objects.requireNonNull(spec, "spec");
        if (spec.walks() < 1) {
            throw new IllegalArgumentException("walks must be >= 1");
        }
        List<String> queries = loadQueries(spec.queriesFile());
        List<String> warmupQueries = spec.warmupQueriesFile() == null
                ? List.of()
                : loadQueries(spec.warmupQueriesFile());
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.kOverride(),
                spec.coverLimit(),
                spec.decompositionTimeoutMs(),
                spec.methodTimeoutMs(),
                index.k());
        int k = runConfig.k();
        int coverLimit = runConfig.coverLimit();
        int decompositionTimeoutMs = runConfig.decompositionTimeoutMs();
        int methodTimeoutMs = runConfig.methodTimeoutMs();
        List<DecompositionMethod> methods = compareFileMethods();

        PrintWriter out = openCompareWriter(spec.outputPath());
        boolean closeOut = spec.outputPath() != null;
        try {
            String command = "estimationbench --index " + spec.indexPath()
                    + " --queries-file " + spec.queriesFile()
                    + " --method-timeout-ms " + methodTimeoutMs
                    + " --decomposition-timeout-ms " + decompositionTimeoutMs
                    + " --cover-limit " + coverLimit
                    + " --k " + k
                    + " --estimate-walks " + spec.walks()
                    + " --seed " + spec.seed();
            if (spec.warmupQueriesFile() != null) {
                command += " --warmup-queries-file " + spec.warmupQueriesFile();
            }
            printEstimationBenchHeader(
                    out,
                    spec,
                    queries.size(),
                    warmupQueries.size(),
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    coverLimit,
                    k,
                    command,
                    config.estimatorType(),
                    config.wanderJoinRequireExtension(),
                    spec.seed(),
                    config.estimationSeed());
            long warmupElapsedNanos = 0L;
            long warmupMethodRows = 0L;
            if (!warmupQueries.isEmpty()) {
                long warmupStartedNanos = System.nanoTime();
                warmupMethodRows = runEstimationBenchWarmup(
                        warmupQueries,
                        methods,
                        coverLimit,
                        k,
                        decompositionTimeoutMs,
                        methodTimeoutMs,
                        spec.walks());
                warmupElapsedNanos = System.nanoTime() - warmupStartedNanos;
            }
            out.println(String.format(
                    Locale.ROOT,
                    "warmup query_count=%d method_rows=%d elapsed_ms=%.3f",
                    warmupQueries.size(),
                    warmupMethodRows,
                    nanosToMillis(warmupElapsedNanos)));
            out.flush();

            long startedNanos = System.nanoTime();
            long methodRows = 0L;
            long stepRows = 0L;
            long okRows = 0L;
            long timeoutRows = 0L;
            long decompTimeoutRows = 0L;
            long noCandidateRows = 0L;
            long errorRows = 0L;
            int queryNumber = 0;

            for (String queryText : queries) {
                queryNumber++;
                List<MethodOutcome<EstimationBenchEvaluation>> outcomes = evaluateQueryAcrossMethods(
                        queryText,
                        methods,
                        coverLimit,
                        k,
                        decompositionTimeoutMs,
                        methodTimeoutMs,
                        (candidate, deadlineNanos) -> evaluateForEstimationBench(candidate, spec.walks(), deadlineNanos));

                for (MethodOutcome<EstimationBenchEvaluation> outcome : outcomes) {
                    methodRows++;
                    DecompositionCandidate candidate = outcome.candidate();
                    switch (outcome.type()) {
                        case PARSE_ERROR -> {
                            stepRows++;
                            emitEstimationBenchRow(out, EstimationBenchRow.parseError(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.wallNanos(),
                                    outcome.errorMessage()));
                            errorRows++;
                        }
                        case MISSING_CANDIDATE -> {
                            stepRows++;
                            emitEstimationBenchRow(out, EstimationBenchRow.withoutCandidate(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos(),
                                    outcome.status()));
                            if ("DECOMP_TIMEOUT".equals(outcome.status())) {
                                decompTimeoutRows++;
                            } else {
                                noCandidateRows++;
                            }
                        }
                        case TIMEOUT -> {
                            stepRows++;
                            emitEstimationBenchRow(out, EstimationBenchRow.timeout(
                                    queryNumber,
                                    outcome.method(),
                                    candidate,
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos()));
                            timeoutRows++;
                        }
                        case SUCCESS -> {
                            EvaluationWithStats evaluation = outcome.evaluation().evaluation();
                            long queryNanos = evaluation.stats().queryNanos();
                            long mappingNanos = evaluation.stats().mappingNanos();
                            long estimateNanos = evaluation.stats().estimateNanos();
                            long joinNanos = evaluation.stats().joinNanos();
                            long totalNanos = queryNanos + mappingNanos + estimateNanos + joinNanos;
                            List<PrefixEstimationStep> steps = outcome.evaluation().steps();
                            if (steps.isEmpty()) {
                                stepRows++;
                                emitEstimationBenchRow(out, EstimationBenchRow.okWithoutSteps(
                                        queryNumber,
                                        outcome.method(),
                                        candidate,
                                        evaluation,
                                        outcome.parseNanos(),
                                        outcome.wallNanos(),
                                        totalNanos));
                            } else {
                                for (PrefixEstimationStep step : steps) {
                                    stepRows++;
                                    emitEstimationBenchRow(out, EstimationBenchRow.okStep(
                                            queryNumber,
                                            outcome.method(),
                                            candidate,
                                            evaluation,
                                            step,
                                            outcome.parseNanos(),
                                            outcome.wallNanos(),
                                            totalNanos));
                                }
                            }
                            okRows++;
                        }
                    }
                }

                if (queryNumber % COMPARE_FILE_PROGRESS_EVERY == 0) {
                    out.println(String.format(
                            Locale.ROOT,
                            "progress_time=%s queries_completed=%d/%d method_rows=%d step_rows=%d approx_pct=%.2f",
                            Instant.now(),
                            queryNumber,
                            queries.size(),
                            methodRows,
                            stepRows,
                            (queryNumber * 100.0d) / Math.max(1, queries.size())));
                    out.flush();
                }
            }

            long elapsedNanos = System.nanoTime() - startedNanos;
            out.println(String.format(
                    Locale.ROOT,
                    "summary query_count=%d method_rows=%d step_rows=%d ok=%d timeout=%d decomp_timeout=%d no_candidate=%d error=%d elapsed_ms=%.3f",
                    queries.size(),
                    methodRows,
                    stepRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    nanosToMillis(elapsedNanos)));
            out.flush();

            return new BenchTypes.EstimationBenchReport(
                    queries.size(),
                    methodRows,
                    stepRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    elapsedNanos);
        } finally {
            closeOwnedWriter(out, closeOut);
        }
    }

    private List<DecompositionMethod> compareFileMethods() {
        Set<DecompositionMethod> plannerMethods = planner.supportedMethods();
        List<DecompositionMethod> methods = new ArrayList<>(plannerMethods);
        methods.sort(Comparator.comparingInt(Enum::ordinal));
        return List.copyOf(methods);
    }

    @FunctionalInterface
    private interface CandidateEvaluator<T> {
        T evaluate(PreparedCandidate candidate, long deadlineNanos);
    }

    private record PreparedMethodSelection(
            PreparedCandidate candidate,
            boolean timedOutWithoutCandidate,
            long decomposeNanos) {
        private String missingStatus() {
            return timedOutWithoutCandidate ? "DECOMP_TIMEOUT" : "NO_CANDIDATE";
        }
    }

    private enum MethodOutcomeType {
        PARSE_ERROR,
        MISSING_CANDIDATE,
        TIMEOUT,
        SUCCESS
    }

    private record MethodOutcome<T>(
            MethodOutcomeType type,
            DecompositionMethod method,
            DecompositionCandidate candidate,
            long parseNanos,
            long decomposeNanos,
            long wallNanos,
            String status,
            String errorMessage,
            T evaluation) {
    }

    private <T> List<MethodOutcome<T>> evaluateQueryAcrossMethods(
            String queryText,
            List<DecompositionMethod> methods,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs,
            CandidateEvaluator<T> evaluator) {
        ParsedQuery parsed = parseQueryTimed(queryText);
        List<MethodOutcome<T>> outcomes = new ArrayList<>(methods.size());
        if (parsed.failed()) {
            for (DecompositionMethod method : methods) {
                outcomes.add(new MethodOutcome<>(
                        MethodOutcomeType.PARSE_ERROR,
                        method,
                        null,
                        parsed.parseNanos(),
                        0L,
                        0L,
                        "ERROR",
                        parsed.errorMessage(),
                        null));
            }
            return List.copyOf(outcomes);
        }
        for (DecompositionMethod method : methods) {
            outcomes.add(executeMethodRun(
                    parsed,
                    method,
                    coverLimit,
                    k,
                    decompositionTimeoutMs,
                    methodTimeoutMs,
                    evaluator));
        }
        return List.copyOf(outcomes);
    }

    private <T> MethodOutcome<T> executeMethodRun(
            ParsedQuery parsed,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs,
            CandidateEvaluator<T> evaluator) {
        long deadlineNanos = Deadline.afterMillis(methodTimeoutMs);
        long methodStartNanos = System.nanoTime();
        PreparedMethodSelection selection = null;
        try {
            selection = selectPreparedCandidateForMethod(
                    parsed.cq(),
                    method,
                    coverLimit,
                    k,
                    decompositionTimeoutMs,
                    deadlineNanos);
            if (selection.candidate() == null) {
                return new MethodOutcome<>(
                        MethodOutcomeType.MISSING_CANDIDATE,
                        method,
                        null,
                        parsed.parseNanos(),
                        selection.decomposeNanos(),
                        System.nanoTime() - methodStartNanos,
                        selection.missingStatus(),
                        null,
                        null);
            }

            T evaluation = evaluator.evaluate(selection.candidate(), deadlineNanos);
            return new MethodOutcome<>(
                    MethodOutcomeType.SUCCESS,
                    method,
                    selection.candidate().candidate(),
                    parsed.parseNanos(),
                    selection.decomposeNanos(),
                    System.nanoTime() - methodStartNanos,
                    "OK",
                    null,
                    evaluation);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new MethodOutcome<>(
                    MethodOutcomeType.TIMEOUT,
                    method,
                    selection == null || selection.candidate() == null ? null : selection.candidate().candidate(),
                    parsed.parseNanos(),
                    selection == null ? 0L : selection.decomposeNanos(),
                    System.nanoTime() - methodStartNanos,
                    "TIMEOUT",
                    null,
                    null);
        }
    }

    private PreparedMethodSelection selectPreparedCandidateForMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            long deadlineNanos) {
        Planner.MethodSelection planned = planner.planMethod(cq, method, coverLimit, k, decompositionTimeoutMs);
        List<DecompositionCandidate> candidates = toBenchCandidates(planned.candidates());
        if (candidates.isEmpty()) {
            return new PreparedMethodSelection(null, planned.timedOut(), planned.decomposeNanos());
        }
        PreparedCandidate selected = pickBestPreparedCandidate(
                candidates,
                config.defaultDecomposeSelectionWalks(),
                config.defaultDecomposeSelectionRandomOrders(),
                1,
                deadlineNanos);
        return new PreparedMethodSelection(selected, planned.timedOut(), planned.decomposeNanos());
    }

    private static void printEstimationBenchHeader(
            PrintWriter out,
            BenchTypes.EstimationBenchSpec spec,
            int totalQueries,
            int warmupQueries,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int k,
            String command,
            EngineConfig.EstimatorType estimatorType,
            boolean wanderJoinRequireExtension,
            long runSeed,
            long estimationSeed) {
        out.println("started=" + Instant.now());
        out.println("index=" + spec.indexPath());
        out.println("queries=" + spec.queriesFile());
        out.println("total_queries=" + totalQueries);
        out.println("warmup_queries=" + (spec.warmupQueriesFile() == null ? "-" : spec.warmupQueriesFile()));
        out.println("warmup_query_count=" + warmupQueries);
        out.println("method_timeout_ms=" + methodTimeoutMs);
        out.println("decomposition_timeout_ms=" + decompositionTimeoutMs);
        out.println("cover_limit=" + coverLimit);
        out.println("k=" + k);
        out.println("seed=" + runSeed);
        out.println("selection_seed=" + estimationSeed);
        out.println("series_parallel_seed=" + estimationSeed);
        out.println("estimator_type=" + estimatorType);
        out.println("estimation_seed=" + estimationSeed);
        out.println("estimate_walks=" + spec.walks());
        out.println("estimate_budget_policy=absolute_n_per_query_method");
        out.println("wanderjoin_require_extension=" + wanderJoinRequireExtension);
        out.println("command=" + command);
        out.println(
                "# columns: query method ord step variable prefix_order full_order estimate stderr actual prefix_estimate_ms prefix_eval_ms prefix_total_ms prefix_cum_estimate_ms prefix_cum_eval_ms prefix_cum_total_ms q_error rel_error cum_q_error cum_rel_error final_answers final_estimate final_stderr parse_ms decompose_ms decomp_estimate_ms wall_ms end_to_end_ms total_ms query_ms mapping_ms estimate_ms join_ms status [error]");
        out.flush();
    }

    private long runEstimationBenchWarmup(
            List<String> warmupQueries,
            List<DecompositionMethod> methods,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs,
            int walks) {
        long warmedMethodRows = 0L;
        for (String queryText : warmupQueries) {
            ConjunctiveQuery cq;
            try {
                cq = parseCQ(queryText);
            } catch (Exception ignored) {
                continue;
            }
            ComparisonCandidates comparison = prepareComparisonCandidates(
                    cq,
                    coverLimit,
                    k,
                    decompositionTimeoutMs);
            Map<DecompositionMethod, DecompositionCandidate> byMethod = new EnumMap<>(DecompositionMethod.class);
            for (DecompositionCandidate candidate : comparison.candidates()) {
                byMethod.put(candidate.method(), candidate);
            }
            for (DecompositionMethod method : methods) {
                DecompositionCandidate candidate = byMethod.get(method);
                if (candidate == null) {
                    continue;
                }
                warmedMethodRows++;
                try {
                    long deadlineNanos = Deadline.afterMillis(methodTimeoutMs);
                    PreparedCandidate prepared = prepareCandidate(candidate, deadlineNanos);
                    evaluateForEstimationBench(prepared, walks, deadlineNanos);
                } catch (RuntimeException ignored) {
                    // Warmup is best-effort and should not fail the measured run.
                }
            }
        }
        return warmedMethodRows;
    }

    private static void printCompareFileHeader(
            PrintWriter compareOut,
            BenchTypes.CompareFileSpec spec,
            int totalQueries,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int k,
            String command,
            long estimationSeed) {
        compareOut.println("started=" + Instant.now());
        compareOut.println("index=" + spec.indexPath());
        compareOut.println("queries=" + spec.queriesFile());
        compareOut.println("total_queries=" + totalQueries);
        compareOut.println("method_timeout_ms=" + methodTimeoutMs);
        compareOut.println("decomposition_timeout_ms=" + decompositionTimeoutMs);
        compareOut.println("cover_limit=" + coverLimit);
        compareOut.println("k=" + k);
        compareOut.println("seed=" + spec.seed());
        compareOut.println("selection_seed=" + estimationSeed);
        compareOut.println("series_parallel_seed=" + estimationSeed);
        compareOut.println("estimation_seed=" + estimationSeed);
        compareOut.println("command=" + command);
        compareOut.flush();
    }

    private static String formatDecompositionLine(
            int queryNumber,
            DecompositionCandidate candidate) {
        return String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d decomposition=\"%s\"",
                queryNumber,
                candidate.method().name(),
                candidate.ordinal(),
                sanitize(formatDecomposition(candidate.decomposition())));
    }

    private static String formatDecomposition(Plan decomposition) {
        StringBuilder builder = new StringBuilder();
        builder.append("projected=").append(decomposition.projectedVariableNames());
        builder.append(" components=[");
        List<Component> components = decomposition.components();
        for (int i = 0; i < components.size(); i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            Component component = components.get(i);
            builder.append(component.sourceVarName())
                    .append("->")
                    .append(component.targetVarName())
                    .append(" d=")
                    .append(component.diameter())
                    .append(" cpq=")
                    .append(component.cpq())
                    .append(" mask=")
                    .append(component.maskUnsafe());
        }
        builder.append("]");
        return builder.toString();
    }

    private record CompareFileRow(
            int queryNumber,
            DecompositionMethod method,
            int ordinal,
            int components,
            int maxDiameter,
            int edgesCollapsed,
            long answers,
            double estimatedCount,
            double estimateStdError,
            long parseNanos,
            long decomposeNanos,
            long decompEstimateNanos,
            long wallNanos,
            long totalNanos,
            long queryNanos,
            long mappingNanos,
            long estimateNanos,
            long joinNanos,
            List<String> variableOrder,
            String status,
            String errorMessage) {
        private static final long UNKNOWN_ANSWER_COUNT = -1L;

        private static CompareFileRow parseError(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long wallNanos,
                String errorMessage) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    0,
                    0,
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    0L,
                    0L,
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    List.of(),
                    "ERROR",
                    errorMessage);
        }

        private static CompareFileRow withoutCandidate(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long decomposeNanos,
                long wallNanos,
                String status) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    0,
                    0,
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    decomposeNanos,
                    0L,
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    List.of(),
                    status,
                    null);
        }

        private static CompareFileRow timeout(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                int edgesCollapsed,
                long parseNanos,
                long decomposeNanos,
                long wallNanos) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    candidate == null ? -1 : candidate.ordinal(),
                    candidate == null ? 0 : candidate.decomposition().size(),
                    candidate == null ? 0 : candidate.decomposition().maxDiameter(),
                    edgesCollapsed,
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    decomposeNanos,
                    candidate == null ? 0L : candidate.selectionEstimateNanos(),
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    List.of(),
                    "TIMEOUT",
                    null);
        }

        private static CompareFileRow ok(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                int edgesCollapsed,
                long parseNanos,
                long wallNanos,
                EvaluationWithStats evaluation) {
            long queryNanos = evaluation.stats().queryNanos();
            long mappingNanos = evaluation.stats().mappingNanos();
            long estimateNanos = evaluation.stats().estimateNanos();
            long joinNanos = evaluation.stats().joinNanos();
            return new CompareFileRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    candidate.decomposition().size(),
                    candidate.decomposition().maxDiameter(),
                    edgesCollapsed,
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    parseNanos,
                    candidate.decomposeNanos(),
                    candidate.selectionEstimateNanos(),
                    wallNanos,
                    queryNanos + mappingNanos + estimateNanos + joinNanos,
                    queryNanos,
                    mappingNanos,
                    estimateNanos,
                    joinNanos,
                    evaluation.variableOrder(),
                    "OK",
                    null);
        }
    }

    private static void emitCompareFileRow(
            PrintWriter out,
            CompareFileRow row) {
        String errorSegment = row.errorMessage() == null || row.errorMessage().isBlank()
                ? ""
                : String.format(Locale.ROOT, " error=\"%s\"", sanitize(row.errorMessage()));
        out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d var_order=%s comps=%d max_diam=%d edges_collapsed=%d answers=%d estimate=%.6f stderr=%.6f parse_ms=%.3f decompose_ms=%.3f decomp_estimate_ms=%.3f wall_ms=%.3f end_to_end_ms=%.3f total_ms=%.3f query_ms=%.3f mapping_ms=%.3f estimate_ms=%.3f join_ms=%.3f status=%s%s",
                row.queryNumber(),
                row.method().name(),
                row.ordinal(),
                formatVariableOrder(row.variableOrder()),
                row.components(),
                row.maxDiameter(),
                row.edgesCollapsed(),
                row.answers(),
                row.estimatedCount(),
                row.estimateStdError(),
                nanosToMillis(row.parseNanos()),
                nanosToMillis(row.decomposeNanos()),
                nanosToMillis(row.decompEstimateNanos()),
                nanosToMillis(row.wallNanos()),
                nanosToMillis(row.parseNanos() + row.wallNanos()),
                nanosToMillis(row.totalNanos()),
                nanosToMillis(row.queryNanos()),
                nanosToMillis(row.mappingNanos()),
                nanosToMillis(row.estimateNanos()),
                nanosToMillis(row.joinNanos()),
                row.status(),
                errorSegment));
    }

    private record EstimationBenchRow(
            int queryNumber,
            DecompositionMethod method,
            int ordinal,
            int step,
            String variable,
            List<String> prefixOrder,
            List<String> fullOrder,
            double estimate,
            double standardError,
            long actual,
            long prefixEstimateNanos,
            long prefixEvalNanos,
            long prefixTotalNanos,
            long prefixCumulativeEstimateNanos,
            long prefixCumulativeEvalNanos,
            long prefixCumulativeTotalNanos,
            double qError,
            double relativeError,
            double cumulativeQError,
            double cumulativeRelativeError,
            long finalAnswers,
            double finalEstimate,
            double finalStdError,
            long parseNanos,
            long decomposeNanos,
            long decompEstimateNanos,
            long wallNanos,
            long totalNanos,
            long queryNanos,
            long mappingNanos,
            long estimateNanos,
            long joinNanos,
            String status,
            String errorMessage) {
        private static final long UNKNOWN_COUNT = -1L;

        private static EstimationBenchRow parseError(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long wallNanos,
                String errorMessage) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    0L,
                    0L,
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    "ERROR",
                    errorMessage);
        }

        private static EstimationBenchRow withoutCandidate(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long decomposeNanos,
                long wallNanos,
                String status) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    decomposeNanos,
                    0L,
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    status,
                    null);
        }

        private static EstimationBenchRow timeout(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                long parseNanos,
                long decomposeNanos,
                long wallNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate == null ? -1 : candidate.ordinal(),
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    parseNanos,
                    decomposeNanos,
                    candidate == null ? 0L : candidate.selectionEstimateNanos(),
                    wallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    "TIMEOUT",
                    null);
        }

        private static EstimationBenchRow okWithoutSteps(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                EvaluationWithStats evaluation,
                long parseNanos,
                long wallNanos,
                long totalNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    0,
                    null,
                    List.of(),
                    evaluation.variableOrder(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    parseNanos,
                    candidate.decomposeNanos(),
                    candidate.selectionEstimateNanos(),
                    wallNanos,
                    totalNanos,
                    evaluation.stats().queryNanos(),
                    evaluation.stats().mappingNanos(),
                    evaluation.stats().estimateNanos(),
                    evaluation.stats().joinNanos(),
                    "OK",
                    null);
        }

        private static EstimationBenchRow okStep(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                EvaluationWithStats evaluation,
                PrefixEstimationStep step,
                long parseNanos,
                long wallNanos,
                long totalNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    step.step(),
                    step.variable(),
                    step.prefixOrder(),
                    evaluation.variableOrder(),
                    step.estimate(),
                    step.standardError(),
                    step.actual(),
                    step.estimateNanos(),
                    step.evalNanos(),
                    step.prefixNanos(),
                    step.cumulativeEstimateNanos(),
                    step.cumulativeEvalNanos(),
                    step.cumulativePrefixNanos(),
                    step.qError(),
                    step.relativeError(),
                    step.cumulativeQError(),
                    step.cumulativeRelativeError(),
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    parseNanos,
                    candidate.decomposeNanos(),
                    candidate.selectionEstimateNanos(),
                    wallNanos,
                    totalNanos,
                    evaluation.stats().queryNanos(),
                    evaluation.stats().mappingNanos(),
                    evaluation.stats().estimateNanos(),
                    evaluation.stats().joinNanos(),
                    "OK",
                    null);
        }
    }

    private static void emitEstimationBenchRow(
            PrintWriter out,
            EstimationBenchRow row) {
        String safeVariable = row.variable() == null ? "-" : row.variable();
        String errorSegment = row.errorMessage() == null || row.errorMessage().isBlank()
                ? ""
                : String.format(Locale.ROOT, " error=\"%s\"", sanitize(row.errorMessage()));
        out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d step=%d variable=%s prefix_order=%s full_order=%s estimate=%.6f stderr=%.6f actual=%d prefix_estimate_ms=%.3f prefix_eval_ms=%.3f prefix_total_ms=%.3f prefix_cum_estimate_ms=%.3f prefix_cum_eval_ms=%.3f prefix_cum_total_ms=%.3f q_error=%.6f rel_error=%.6f cum_q_error=%.6f cum_rel_error=%.6f final_answers=%d final_estimate=%.6f final_stderr=%.6f parse_ms=%.3f decompose_ms=%.3f decomp_estimate_ms=%.3f wall_ms=%.3f end_to_end_ms=%.3f total_ms=%.3f query_ms=%.3f mapping_ms=%.3f estimate_ms=%.3f join_ms=%.3f status=%s%s",
                row.queryNumber(),
                row.method().name(),
                row.ordinal(),
                row.step(),
                safeVariable,
                formatVariableOrder(row.prefixOrder()),
                formatVariableOrder(row.fullOrder()),
                row.estimate(),
                row.standardError(),
                row.actual(),
                nanosToMillis(row.prefixEstimateNanos()),
                nanosToMillis(row.prefixEvalNanos()),
                nanosToMillis(row.prefixTotalNanos()),
                nanosToMillis(row.prefixCumulativeEstimateNanos()),
                nanosToMillis(row.prefixCumulativeEvalNanos()),
                nanosToMillis(row.prefixCumulativeTotalNanos()),
                row.qError(),
                row.relativeError(),
                row.cumulativeQError(),
                row.cumulativeRelativeError(),
                row.finalAnswers(),
                row.finalEstimate(),
                row.finalStdError(),
                nanosToMillis(row.parseNanos()),
                nanosToMillis(row.decomposeNanos()),
                nanosToMillis(row.decompEstimateNanos()),
                nanosToMillis(row.wallNanos()),
                nanosToMillis(row.parseNanos() + row.wallNanos()),
                nanosToMillis(row.totalNanos()),
                nanosToMillis(row.queryNanos()),
                nanosToMillis(row.mappingNanos()),
                nanosToMillis(row.estimateNanos()),
                nanosToMillis(row.joinNanos()),
                row.status(),
                errorSegment));
    }

    private static String formatVariableOrder(List<String> variableOrder) {
        if (variableOrder == null || variableOrder.isEmpty()) {
            return "[]";
        }
        return "[" + String.join(",", variableOrder) + "]";
    }

    private static int edgesCollapsed(Plan decomposition) {
        int collapsed = 0;
        for (Component component : decomposition.components()) {
            collapsed += Math.max(0, component.maskUnsafe().cardinality() - 1);
        }
        return collapsed;
    }

    private static List<String> loadQueries(Path queriesFile) throws Exception {
        List<String> lines = Files.readAllLines(queriesFile, StandardCharsets.UTF_8);
        List<String> queries = new ArrayList<>(lines.size());
        for (String line : lines) {
            String query = line.trim();
            if (query.isEmpty() || query.startsWith("#")) {
                continue;
            }
            queries.add(query);
        }
        return queries;
    }

    private static PrintWriter openCompareWriter(Path path) throws Exception {
        if (path == null) {
            return new PrintWriter(System.out, true);
        }
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        return new PrintWriter(writer, true);
    }

    private static void closeOwnedWriter(PrintWriter writer, boolean owned) {
        if (writer == null) {
            return;
        }
        if (owned) {
            writer.close();
        } else {
            writer.flush();
        }
    }

    private static PrintWriter openDecompositionWriter(Path path) throws Exception {
        if (path == null) {
            return null;
        }
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        return new PrintWriter(writer, true);
    }

    private static String sanitize(String text) {
        return text.replace('\"', '\'');
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static QueryRunConfig normalizeRunConfig(
            int kOverride,
            int coverLimit,
            int decompositionTimeoutMs,
            int methodTimeoutMs,
            int indexK) {
        return new QueryRunConfig(
                kOverride > 0 ? kOverride : indexK,
                coverLimit,
                Math.max(0, decompositionTimeoutMs),
                Math.max(0, methodTimeoutMs));
    }

    private record QueryRunConfig(
            int k,
            int coverLimit,
            int decompositionTimeoutMs,
            int methodTimeoutMs) {
    }

    /**
     * Runs the profile workflow on the default decomposition.
     */
    public BenchTypes.ProfileReport profile(BenchTypes.ProfileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        Plan decomposition = decompose(cq);
        try {
            OrderProfileSummary summary = profileOrders(
                    decomposition,
                    Math.max(0, spec.profileOrders()),
                    spec.seed(),
                    Deadline.afterMillis(Math.max(0, spec.profileTimeoutMs())));
            return new BenchTypes.ProfileReport(summary.profiles().size(), false);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new BenchTypes.ProfileReport(0, true);
        }
    }

    /**
     * Runs the estimate workflow on the default decomposition.
     */
    public BenchTypes.EstimateReport estimate(BenchTypes.EstimateSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        Plan decomposition = decompose(cq);
        try {
            CardinalityEstimate estimate = estimateCount(
                    decomposition,
                    Math.max(1, spec.walks()),
                    spec.seed(),
                    Deadline.afterMillis(Math.max(0, spec.methodTimeoutMs())));
            return new BenchTypes.EstimateReport(
                    estimate.estimatedCount(),
                    estimate.standardError(),
                    false);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new BenchTypes.EstimateReport(Double.NaN, Double.NaN, true);
        }
    }

    Plan decompose(ConjunctiveQuery cq) {
        Objects.requireNonNull(cq, "cq");
        if (!config.defaultDecomposeUseBest()) {
            return planner.decompose(cq);
        }

        List<DecompositionCandidate> candidates = selectBestCandidates(
                cq,
                config.defaultDecomposeCoverLimit(),
                index.k(),
                0);
        if (candidates.isEmpty()) {
            return planner.decompose(cq);
        }
        return pickBestDefaultCandidate(candidates).decomposition();
    }

    List<DecompositionCandidate> decomposeAll(ConjunctiveQuery cq, int coverLimit) {
        return toBenchCandidates(planner.planAll(cq, coverLimit));
    }

    List<DecompositionCandidate> decomposeAll(ConjunctiveQuery cq, int coverLimit, int k) {
        return toBenchCandidates(planner.planAll(cq, coverLimit, k));
    }

    List<DecompositionCandidate> decomposeAll(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        return toBenchCandidates(planner.planAll(cq, coverLimit, k, decompositionTimeoutMs).candidates());
    }

    ComponentFilteredCandidates decomposeAllWithComponentFilter(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int minComponents,
            int maxComponents) {
        List<DecompositionCandidate> all = decomposeAll(cq, coverLimit, k, decompositionTimeoutMs);
        if (all.isEmpty()) {
            return new ComponentFilteredCandidates(List.of(), List.of());
        }
        List<DecompositionCandidate> filtered = new java.util.ArrayList<>();
        for (DecompositionCandidate candidate : all) {
            int comps = candidate.decomposition().size();
            if (minComponents > 0 && comps < minComponents) {
                continue;
            }
            if (maxComponents > 0 && comps > maxComponents) {
                continue;
            }
            filtered.add(candidate);
        }
        return new ComponentFilteredCandidates(List.copyOf(all), List.copyOf(filtered));
    }

    ExploreCandidates prepareExploreCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int minComponents,
            int maxComponents,
            int candidateLimit) {
        ComponentFilteredCandidates filtered = decomposeAllWithComponentFilter(
                cq,
                coverLimit,
                k,
                decompositionTimeoutMs,
                minComponents,
                maxComponents);
        if (filtered.allCandidates().isEmpty()) {
            return new ExploreCandidates(
                    ExplorePreparationStatus.NO_DECOMPOSITIONS,
                    List.of(),
                    List.of(),
                    List.of());
        }
        if (filtered.filteredCandidates().isEmpty()) {
            return new ExploreCandidates(
                    ExplorePreparationStatus.NO_DECOMPOSITIONS_AFTER_COMPONENT_FILTER,
                    filtered.allCandidates(),
                    List.of(),
                    List.of());
        }
        List<DecompositionCandidate> selected = filtered.filteredCandidates();
        if (candidateLimit > 0 && selected.size() > candidateLimit) {
            selected = selected.subList(0, candidateLimit);
        }
        return new ExploreCandidates(
                ExplorePreparationStatus.READY,
                filtered.allCandidates(),
                filtered.filteredCandidates(),
                List.copyOf(selected));
    }

    List<DecompositionEvaluation> evaluateAll(
            ConjunctiveQuery cq,
            EvaluationMode mode,
            int coverLimit) {
        return evaluateAll(cq, mode, coverLimit, index.k());
    }

    /**
     * Selects one indexable decomposition candidate per method using the
     * configured selection policy (estimation or component-cost ranking).
     *
     * @param cq         Query to decompose.
     * @param coverLimit Maximum number of exact covers to consider per method.
     * @return Selected candidates, one per method when available.
     */
    List<DecompositionCandidate> selectBestCandidates(ConjunctiveQuery cq, int coverLimit) {
        return selectBestCandidates(cq, coverLimit, index.k(), 0);
    }

    /**
     * Selects one indexable decomposition candidate per method by estimating
     * each candidate with the same Wander Join budget.
     *
     * @param cq         Query to decompose.
     * @param coverLimit Maximum number of exact covers to consider per method.
     * @param k          Maximum CPQ diameter to allow when decomposing.
     * @return Selected candidates, one per method when available.
     */
    List<DecompositionCandidate> selectBestCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k) {
        return selectBestCandidates(cq, coverLimit, k, 0);
    }

    List<DecompositionCandidate> selectBestCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        return selectBestCandidatesWithTimeoutInfo(cq, coverLimit, k, decompositionTimeoutMs).candidates();
    }

    CandidateSelection selectBestCandidatesWithTimeoutInfo(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        Planner.Selection planned = planner.planAll(cq, coverLimit, k, decompositionTimeoutMs);
        List<DecompositionCandidate> allCandidates = toBenchCandidates(planned.candidates());
        List<DecompositionCandidate> selected = pickBestByMethodWithEstimation(allCandidates);
        return new CandidateSelection(
                selected,
                Set.copyOf(planned.timedOutMethods()),
                Map.copyOf(planned.decompositionNanosByMethod()));
    }

    private PreparedCandidateSelection selectBestPreparedCandidatesWithTimeoutInfo(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            long deadlineNanos) {
        Planner.Selection planned = planner.planAll(cq, coverLimit, k, decompositionTimeoutMs);
        List<DecompositionCandidate> allCandidates = toBenchCandidates(planned.candidates());
        List<PreparedCandidate> selected = pickBestPreparedByMethodWithEstimation(allCandidates, deadlineNanos);
        return new PreparedCandidateSelection(
                selected,
                Set.copyOf(planned.timedOutMethods()),
                Map.copyOf(planned.decompositionNanosByMethod()));
    }

    private record ParsedQuery(
            ConjunctiveQuery cq,
            long parseNanos,
            String errorMessage) {
        private boolean failed() {
            return errorMessage != null;
        }
    }

    private ParsedQuery parseQueryTimed(String queryText) {
        long parseStart = System.nanoTime();
        try {
            return new ParsedQuery(parseCQ(queryText), System.nanoTime() - parseStart, null);
        } catch (Exception ex) {
            return new ParsedQuery(null, System.nanoTime() - parseStart, summarizeError(ex));
        }
    }

    ComparisonCandidates prepareComparisonCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        CandidateSelection selection = selectBestCandidatesWithTimeoutInfo(cq, coverLimit, k, decompositionTimeoutMs);
        EnumSet<DecompositionMethod> emittedMethods = EnumSet.noneOf(DecompositionMethod.class);
        for (DecompositionCandidate candidate : selection.candidates()) {
            emittedMethods.add(candidate.method());
        }
        EnumSet<DecompositionMethod> timedOutWithoutCandidate = EnumSet.noneOf(DecompositionMethod.class);
        timedOutWithoutCandidate.addAll(selection.timedOutMethods());
        timedOutWithoutCandidate.removeAll(emittedMethods);
        ComparisonPreparationStatus status = (selection.candidates().isEmpty() && selection.timedOutMethods().isEmpty())
                ? ComparisonPreparationStatus.NO_DECOMPOSITIONS
                : ComparisonPreparationStatus.READY;
        return new ComparisonCandidates(
                status,
                selection.candidates(),
                Set.copyOf(timedOutWithoutCandidate),
                Map.copyOf(selection.decompositionNanosByMethod()));
    }

    List<DecompositionEvaluation> evaluateAll(
            ConjunctiveQuery cq,
            EvaluationMode mode,
            int coverLimit,
            int k) {
        return evaluateAll(cq, mode, coverLimit, k, 0);
    }

    List<DecompositionEvaluation> evaluateAll(
            ConjunctiveQuery cq,
            EvaluationMode mode,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(mode, "mode");
        PreparedCandidateSelection selected = selectBestPreparedCandidatesWithTimeoutInfo(
                cq,
                coverLimit,
                k,
                decompositionTimeoutMs,
                Long.MAX_VALUE);
        List<DecompositionEvaluation> evaluations = new java.util.ArrayList<>(selected.candidates().size());
        for (PreparedCandidate candidate : selected.candidates()) {
            EvaluationWithStats evaluation = evaluateWithStats(candidate.executable(), mode, Long.MAX_VALUE);
            evaluations.add(new DecompositionEvaluation(
                    candidate.candidate().method(),
                    candidate.candidate().ordinal(),
                    candidate.candidate().decomposition(),
                    candidate.candidate().decomposeNanos(),
                    evaluation));
        }
        return evaluations;
    }

    private record PreparedCandidate(
            DecompositionCandidate candidate,
            ExecutablePlan executable) {
    }

    private record CandidateContext(
            PreparedCandidate candidate,
            List<String> projected) {
    }

    private record CandidateEstimate(double conservativeCount) {
    }

    private record CandidateCost(long totalCost, int components, int maxDiameter) {
    }

    private record OrderEstimate(
            List<String> order,
            double conservativeCount,
            double estimatedCount,
            double standardError) {
    }

    private record PrefixEstimationStep(
            int step,
            String variable,
            List<String> prefixOrder,
            double estimate,
            double standardError,
            long actual,
            long estimateNanos,
            long evalNanos,
            long prefixNanos,
            long cumulativeEstimateNanos,
            long cumulativeEvalNanos,
            long cumulativePrefixNanos,
            double qError,
            double relativeError,
            double cumulativeQError,
            double cumulativeRelativeError) {
    }

    private record EstimationBenchEvaluation(
            EvaluationWithStats evaluation,
            List<PrefixEstimationStep> steps) {
    }

    private record PreparedCandidateSelection(
            List<PreparedCandidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    private List<DecompositionCandidate> pickBestByMethodWithEstimation(List<DecompositionCandidate> candidates) {
        Map<DecompositionMethod, List<DecompositionCandidate>> byMethod = new EnumMap<>(DecompositionMethod.class);
        for (DecompositionCandidate candidate : candidates) {
            byMethod.computeIfAbsent(candidate.method(), ignored -> new java.util.ArrayList<>()).add(candidate);
        }

        List<DecompositionCandidate> selected = new java.util.ArrayList<>(byMethod.size());
        for (List<DecompositionCandidate> methodCandidates : byMethod.values()) {
            selected.add(pickBestCandidate(
                    methodCandidates,
                    config.defaultDecomposeSelectionWalks(),
                    config.defaultDecomposeSelectionRandomOrders(),
                    1));
        }
        return selected;
    }

    private List<PreparedCandidate> pickBestPreparedByMethodWithEstimation(
            List<DecompositionCandidate> candidates,
            long deadlineNanos) {
        Map<DecompositionMethod, List<DecompositionCandidate>> byMethod = new EnumMap<>(DecompositionMethod.class);
        for (DecompositionCandidate candidate : candidates) {
            byMethod.computeIfAbsent(candidate.method(), ignored -> new java.util.ArrayList<>()).add(candidate);
        }

        List<PreparedCandidate> selected = new java.util.ArrayList<>(byMethod.size());
        for (List<DecompositionCandidate> methodCandidates : byMethod.values()) {
            selected.add(pickBestPreparedCandidate(
                    methodCandidates,
                    config.defaultDecomposeSelectionWalks(),
                    config.defaultDecomposeSelectionRandomOrders(),
                    1,
                    deadlineNanos));
        }
        return List.copyOf(selected);
    }

    private DecompositionCandidate pickBestDefaultCandidate(List<DecompositionCandidate> candidates) {
        return pickBestCandidate(
                candidates,
                config.defaultDecomposeSelectionWalks(),
                config.defaultDecomposeSelectionRandomOrders(),
                2);
    }

    private DecompositionCandidate pickBestCandidate(
            List<DecompositionCandidate> candidates,
            int walks,
            int randomOrders,
            int phase) {
        if (walks < 1) {
            return pickHeuristicCandidate(candidates);
        }
        return pickBestEstimatedCandidate(candidates, walks, randomOrders, phase);
    }

    private PreparedCandidate pickBestPreparedCandidate(
            List<DecompositionCandidate> candidates,
            int walks,
            int randomOrders,
            int phase,
            long deadlineNanos) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }
        if (walks < 1 || candidates.size() == 1) {
            return prepareCandidate(pickHeuristicCandidate(candidates), deadlineNanos);
        }
        return pickBestPreparedEstimatedCandidate(candidates, walks, randomOrders, phase, deadlineNanos);
    }

    private DecompositionCandidate pickBestEstimatedCandidate(
            List<DecompositionCandidate> candidates,
            int walks,
            int randomOrders,
            int phase) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }
        if (candidates.size() == 1) {
            return candidates.get(0).withSelectionEstimateNanos(0L);
        }

        long selectionEstimateStart = System.nanoTime();
        List<CandidateContext> contexts = new java.util.ArrayList<>(candidates.size());
        for (DecompositionCandidate candidate : candidates) {
            PreparedCandidate prepared = prepareCandidate(candidate, Long.MAX_VALUE);
            List<String> projected = prepared.executable().plan().projectedVariableNames();
            contexts.add(new CandidateContext(prepared, projected));
        }

        DecompositionCandidate best = null;
        CandidateEstimate bestEstimate = null;
        for (int i = 0; i < contexts.size(); i++) {
            CandidateContext context = contexts.get(i);
            CandidateEstimate estimate = estimateCandidate(context, walks, randomOrders, phase, i, Long.MAX_VALUE);
            if (best == null) {
                best = context.candidate().candidate();
                bestEstimate = estimate;
                continue;
            }
            int cmp = compareCandidateEstimate(estimate, bestEstimate);
            if (cmp < 0 || (cmp == 0 && compareCandidateIdentity(context.candidate().candidate(), best) < 0)) {
                best = context.candidate().candidate();
                bestEstimate = estimate;
            }
        }
        long selectionEstimateNanos = System.nanoTime() - selectionEstimateStart;
        return best.withSelectionEstimateNanos(selectionEstimateNanos);
    }

    private PreparedCandidate pickBestPreparedEstimatedCandidate(
            List<DecompositionCandidate> candidates,
            int walks,
            int randomOrders,
            int phase,
            long deadlineNanos) {
        long selectionEstimateStart = System.nanoTime();
        List<CandidateContext> contexts = new java.util.ArrayList<>(candidates.size());
        for (DecompositionCandidate candidate : candidates) {
            PreparedCandidate prepared = prepareCandidate(candidate, deadlineNanos);
            List<String> projected = prepared.executable().plan().projectedVariableNames();
            contexts.add(new CandidateContext(prepared, projected));
        }

        CandidateContext best = null;
        CandidateEstimate bestEstimate = null;
        for (int i = 0; i < contexts.size(); i++) {
            CandidateContext context = contexts.get(i);
            CandidateEstimate estimate = estimateCandidate(context, walks, randomOrders, phase, i, deadlineNanos);
            if (best == null) {
                best = context;
                bestEstimate = estimate;
                continue;
            }
            int cmp = compareCandidateEstimate(estimate, bestEstimate);
            if (cmp < 0 || (cmp == 0 && compareCandidateIdentity(context.candidate().candidate(), best.candidate().candidate()) < 0)) {
                best = context;
                bestEstimate = estimate;
            }
        }

        long selectionEstimateNanos = System.nanoTime() - selectionEstimateStart;
        DecompositionCandidate selected = best.candidate().candidate().withSelectionEstimateNanos(selectionEstimateNanos);
        return new PreparedCandidate(selected, best.candidate().executable());
    }

    /**
     * Keeps the method-native heuristic order when CE-based reranking is disabled.
     * Candidate ordinals are assigned in generation order inside each method.
     */
    private DecompositionCandidate pickHeuristicCandidate(List<DecompositionCandidate> candidates) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }
        DecompositionCandidate best = candidates.get(0);
        for (int i = 1; i < candidates.size(); i++) {
            DecompositionCandidate candidate = candidates.get(i);
            if (compareCandidateIdentity(candidate, best) < 0) {
                best = candidate;
            }
        }
        return best.withSelectionEstimateNanos(0L);
    }

    private CandidateEstimate estimateCandidate(
            CandidateContext context,
            int walks,
            int randomOrders,
            int phase,
            int position,
            long deadlineNanos) {
        if (context.candidate().executable().isEmpty()) {
            return new CandidateEstimate(0.0);
        }

        long seed = mixSeed(
                config.estimationSeed(),
                context.candidate().candidate().method().ordinal(),
                context.candidate().candidate().ordinal(),
                phase,
                position);
        OrderEstimate orderEstimate = joinOrderSelector.estimateBestJoinOrder(
                context.candidate().candidate().decomposition(),
                context.candidate().executable().plan().components(),
                context.candidate().executable().componentCounts(),
                context.candidate().executable().relations(),
                context.projected(),
                walks,
                randomOrders,
                seed,
                deadlineNanos);
        return new CandidateEstimate(orderEstimate.conservativeCount());
    }

    private static int compareCandidateEstimate(CandidateEstimate left, CandidateEstimate right) {
        return Double.compare(left.conservativeCount(), right.conservativeCount());
    }

    private CandidateCost candidateCost(Plan decomposition) {
        long totalCost = 0L;
        for (Component component : decomposition.components()) {
            totalCost = safeAddCost(totalCost, normalizeCost(index.cost(component.cpq())));
        }
        return new CandidateCost(totalCost, decomposition.size(), decomposition.maxDiameter());
    }

    private static int compareCandidateCost(CandidateCost left, CandidateCost right) {
        int cmp = Long.compare(left.totalCost(), right.totalCost());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.components(), right.components());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(left.maxDiameter(), right.maxDiameter());
    }

    private static long normalizeCost(long rawCost) {
        return rawCost < 0L ? 0L : rawCost;
    }

    private static long safeAddCost(long left, long right) {
        if (left >= Long.MAX_VALUE || right >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static int compareCandidateIdentity(DecompositionCandidate left, DecompositionCandidate right) {
        int cmp = Integer.compare(left.method().ordinal(), right.method().ordinal());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(left.ordinal(), right.ordinal());
    }

    private static EvaluationResult emptyResult(EvaluationMode mode) {
        return mode == EvaluationMode.ROWS ? new RowResult(List.of()) : new CountResult(0L);
    }

    private static EvaluationWithStats emptyWithStats(EvaluationMode mode, EvaluationStats stats) {
        return new EvaluationWithStats(emptyResult(mode), stats, List.of(), Double.NaN, Double.NaN);
    }

    private static EvaluationStats fromCompilationStats(ExecutablePlan.CompilationStats compilationStats) {
        EvaluationStats stats = new EvaluationStats();
        stats.addQueryNanos(compilationStats.queryNanos());
        stats.addMappingNanos(compilationStats.mappingNanos());
        return stats;
    }

    EvaluationResult evaluate(Plan decomposition, EvaluationMode mode) {
        return evaluateWithStats(decomposition, mode).result();
    }

    EvaluationWithStats evaluateWithStats(Plan decomposition, EvaluationMode mode) {
        return evaluateWithStats(decomposition, mode, Long.MAX_VALUE);
    }

    EvaluationWithStats evaluateWithStats(Plan decomposition, EvaluationMode mode, long deadlineNanos) {
        Objects.requireNonNull(decomposition, "decomposition");
        Objects.requireNonNull(mode, "mode");

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index, deadlineNanos);
        return evaluateWithStats(executable, mode, deadlineNanos);
    }

    private EvaluationWithStats evaluateWithStats(
            ExecutablePlan executable,
            EvaluationMode mode,
            long deadlineNanos) {
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return emptyWithStats(mode, stats);
        }

        List<Component> components = executable.plan().components();
        List<Long> resultCounts = executable.componentCounts();
        int orderWalks = config.orderEstimationWalks();
        List<String> order;
        double estimatedCount = Double.NaN;
        double estimateStdError = Double.NaN;
        if (orderWalks > 0) {
            int randomOrders = config.orderEstimationRandomOrders();
            List<Relation> relations = executable.relations();
            List<String> projected = executable.plan().projectedVariableNames();
            long estimateStart = System.nanoTime();
            OrderEstimate orderEstimate = joinOrderSelector.estimateBestJoinOrder(
                    executable.plan(),
                    components,
                    resultCounts,
                    relations,
                    projected,
                    orderWalks,
                    randomOrders,
                    config.estimationSeed(),
                    deadlineNanos);
            stats.addEstimateNanos(System.nanoTime() - estimateStart);
            order = orderEstimate.order();
            estimatedCount = orderEstimate.estimatedCount();
            estimateStdError = orderEstimate.standardError();
        } else {
            order = orderByComponentCounts(components, resultCounts);
        }
        Deadline.check(deadlineNanos);
        long joinStart = System.nanoTime();
        EvaluationResult result = switch (mode) {
            case ROWS -> {
                LeapfrogJoin.JoinResult.Rows rows = (LeapfrogJoin.JoinResult.Rows) executable.join(
                        order,
                        LeapfrogJoin.JoinMode.PROJECTED_ROWS,
                        config.joinSafeDistinctFastPath(),
                        deadlineNanos);
                yield new RowResult(rows.rows());
            }
            case COUNT -> {
                LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                        order,
                        LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                        config.joinSafeDistinctFastPath(),
                        deadlineNanos);
                yield new CountResult(count.count());
            }
        };
        stats.addJoinNanos(System.nanoTime() - joinStart);
        return new EvaluationWithStats(result, stats, List.copyOf(order), estimatedCount, estimateStdError);
    }

    private EstimationBenchEvaluation evaluateForEstimationBench(
            PreparedCandidate candidate,
            int walks,
            long deadlineNanos) {
        EvaluationWithStats evaluation = evaluateWithStats(candidate.executable(), EvaluationMode.COUNT, deadlineNanos);
        if (evaluation.result() instanceof CountResult count && count.count() <= 0L) {
            return new EstimationBenchEvaluation(evaluation, List.of());
        }
        List<PrefixEstimationStep> steps = tracePrefixEstimationSteps(
                candidate,
                evaluation.variableOrder(),
                Math.max(1, walks),
                deadlineNanos);
        return new EstimationBenchEvaluation(evaluation, steps);
    }

    private List<PrefixEstimationStep> tracePrefixEstimationSteps(
            PreparedCandidate candidate,
            List<String> variableOrder,
            int walks) {
        return tracePrefixEstimationSteps(candidate, variableOrder, walks, Long.MAX_VALUE);
    }

    private List<PrefixEstimationStep> tracePrefixEstimationSteps(
            PreparedCandidate candidate,
            List<String> variableOrder,
            int walks,
            long deadlineNanos) {
        if (variableOrder.isEmpty()) {
            return List.of();
        }
        ExecutablePlan executable = candidate.executable();
        if (executable.isEmpty()) {
            return List.of();
        }

        List<Relation> relations = executable.relations();
        List<PrefixEstimationStep> steps = new ArrayList<>(variableOrder.size());
        long seedBase = mixSeed(
                config.estimationSeed(),
                candidate.candidate().method().ordinal(),
                candidate.candidate().ordinal(),
                walks,
                variableOrder.hashCode());
        List<WanderJoinEstimator.PrefixEstimate> prefixEstimates = estimatePrefixProjectedCounts(
                relations,
                variableOrder,
                walks,
                seedBase,
                deadlineNanos);
        long cumulativeEstimateNanos = 0L;
        long cumulativeEvalNanos = 0L;
        long cumulativePrefixNanos = 0L;
        double cumulativeQ = 0.0D;
        double cumulativeRelative = 0.0D;
        for (int i = 0; i < variableOrder.size(); i++) {
            Deadline.check(deadlineNanos);
            List<String> prefix = List.copyOf(variableOrder.subList(0, i + 1));
            WanderJoinEstimator.PrefixEstimate prefixEstimate = prefixEstimates.get(i);
            long estimateNanos = prefixEstimate.estimateNanos();
            long actualStart = System.nanoTime();
            long actual = evalProjectedCount(relations, variableOrder, prefix, deadlineNanos);
            long evalNanos = System.nanoTime() - actualStart;
            long prefixNanos = estimateNanos + evalNanos;
            cumulativeEstimateNanos += estimateNanos;
            cumulativeEvalNanos += evalNanos;
            cumulativePrefixNanos += prefixNanos;
            double qError = qError(prefixEstimate.estimatedCount(), actual);
            double relativeError = relativeError(prefixEstimate.estimatedCount(), actual);
            cumulativeQ += qError;
            cumulativeRelative += relativeError;
            double stepCount = i + 1.0D;
            steps.add(new PrefixEstimationStep(
                    i + 1,
                    variableOrder.get(i),
                    prefix,
                    prefixEstimate.estimatedCount(),
                    prefixEstimate.standardError(),
                    actual,
                    estimateNanos,
                    evalNanos,
                    prefixNanos,
                    cumulativeEstimateNanos,
                    cumulativeEvalNanos,
                    cumulativePrefixNanos,
                    qError,
                    relativeError,
                    cumulativeQ / stepCount,
                    cumulativeRelative / stepCount));
        }
        return List.copyOf(steps);
    }

    /**
     * Computes prefix estimates under a fixed absolute estimation budget.
     * WanderJoin uses one shared trial budget across all prefixes; deterministic
     * estimators are evaluated per-prefix.
     */
    private List<WanderJoinEstimator.PrefixEstimate> estimatePrefixProjectedCounts(
            List<Relation> relations,
            List<String> variableOrder,
            int walks,
            long seedBase,
            long deadlineNanos) {
        if (config.estimatorType() == EngineConfig.EstimatorType.WANDERJOIN) {
            return WanderJoinEstimator.estimateProjectedCountPrefixes(
                    relations,
                    variableOrder,
                    walks,
                    seedBase,
                    config.wanderJoinRequireExtension(),
                    deadlineNanos);
        }

        List<WanderJoinEstimator.PrefixEstimate> estimates = new ArrayList<>(variableOrder.size());
        for (int i = 0; i < variableOrder.size(); i++) {
            Deadline.check(deadlineNanos);
            List<String> prefix = List.copyOf(variableOrder.subList(0, i + 1));
            long seed = mixSeed(seedBase, i + 1, prefix.hashCode());
            long estimateStart = System.nanoTime();
            WanderJoinEstimator.Estimate estimate = estimateProjectedCount(
                    relations,
                    variableOrder,
                    prefix,
                    walks,
                    seed,
                    deadlineNanos);
            long estimateNanos = System.nanoTime() - estimateStart;
            estimates.add(new WanderJoinEstimator.PrefixEstimate(
                    estimate.estimatedCount(),
                    estimate.standardError(),
                    estimateNanos));
        }
        return List.copyOf(estimates);
    }

    private long evalProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) LeapfrogJoin.join(
                relations,
                variableOrder,
                projected,
                LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                config.joinSafeDistinctFastPath(),
                deadlineNanos);
        return count.count();
    }

    private static double qError(double estimate, long actual) {
        double smoothedEstimate = Math.max(1.0D, estimate);
        double smoothedActual = Math.max(1.0D, (double) actual);
        return Math.max(smoothedEstimate / smoothedActual, smoothedActual / smoothedEstimate);
    }

    private static double relativeError(double estimate, long actual) {
        return Math.abs(Math.max(0.0D, estimate) - Math.max(0L, actual)) / Math.max(1.0D, actual);
    }

    CardinalityEstimate estimateCount(Plan decomposition, int walks) {
        return estimateCount(decomposition, walks, 0xC0FFEE);
    }

    /**
     * Estimates projected answer cardinality for a prepared decomposition
     * candidate using a stable candidate-derived seed.
     *
     * @param candidate Candidate to estimate.
     * @param walks     Number of random walks to run.
     * @return Estimate with standard error and phase timings.
     */
    CardinalityEstimate estimateCandidateCount(DecompositionCandidate candidate, int walks) {
        Objects.requireNonNull(candidate, "candidate");
        return estimateCount(candidate.decomposition(), walks, candidateSeed(candidate));
    }

    /**
     * Estimates projected answer cardinality via configured estimator.
     *
     * @param decomposition Decomposition to estimate.
     * @param walks         Number of random walks to run.
     * @param seed          Random seed for reproducibility.
     * @return Estimate with standard error and phase timings.
     */
    CardinalityEstimate estimateCount(Plan decomposition, int walks, long seed) {
        return estimateCount(decomposition, walks, seed, Long.MAX_VALUE);
    }

    CardinalityEstimate estimateCount(Plan decomposition, int walks, long seed, long deadlineNanos) {
        Objects.requireNonNull(decomposition, "decomposition");
        if (walks < 1) {
            throw new IllegalArgumentException("walks must be >= 1");
        }

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index, deadlineNanos);
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return new CardinalityEstimate(0.0, 0.0, walks, seed, stats.queryNanos(), stats.mappingNanos(), 0L);
        }

        List<Component> components = executable.plan().components();
        List<Long> resultCounts = executable.componentCounts();
        List<String> order = orderByComponentCounts(components, resultCounts);
        List<String> projected = decomposition.projectedVariableNames();
        List<Relation> relations = executable.relations();

        Deadline.check(deadlineNanos);
        long estimateStart = System.nanoTime();
        WanderJoinEstimator.Estimate estimate = estimateProjectedCount(relations, order, projected, walks, seed, deadlineNanos);
        long estimateNanos = System.nanoTime() - estimateStart;

        return new CardinalityEstimate(
                estimate.estimatedCount(),
                estimate.standardError(),
                walks,
                seed,
                stats.queryNanos(),
                stats.mappingNanos(),
                estimateNanos);
    }

    OrderProfileSummary profileOrders(Plan decomposition, int randomOrders, long seed) {
        return profileOrders(decomposition, randomOrders, seed, Long.MAX_VALUE);
    }

    OrderProfileSummary profileOrders(Plan decomposition, int randomOrders, long seed, long deadlineNanos) {
        Objects.requireNonNull(decomposition, "decomposition");
        if (randomOrders < 0) {
            throw new IllegalArgumentException("randomOrders must be >= 0");
        }

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index, deadlineNanos);
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return new OrderProfileSummary(stats.queryNanos(), stats.mappingNanos(), List.of());
        }

        List<Component> components = executable.plan().components();
        List<Long> resultCounts = executable.componentCounts();
        List<String> defaultOrder = orderByComponentCounts(components, resultCounts);

        List<List<String>> orders = joinOrderSelector.sampleOrders(defaultOrder, randomOrders, seed);

        List<OrderProfile> profiles = new java.util.ArrayList<>(orders.size());
        for (List<String> order : orders) {
            Deadline.check(deadlineNanos);
            long start = System.nanoTime();
            LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                    order,
                    LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                    config.joinSafeDistinctFastPath(),
                    deadlineNanos);
            long joinNanos = System.nanoTime() - start;
            profiles.add(new OrderProfile(order, joinNanos, count.count()));
        }

        return new OrderProfileSummary(stats.queryNanos(), stats.mappingNanos(), profiles);
    }

    /**
     * Profiles join orders for a prepared decomposition candidate using a
     * stable candidate-derived seed.
     *
     * @param candidate    Candidate to profile.
     * @param randomOrders Number of random orders to sample.
     * @return Order profile summary.
     */
    OrderProfileSummary profileCandidateOrders(DecompositionCandidate candidate, int randomOrders) {
        Objects.requireNonNull(candidate, "candidate");
        return profileOrders(candidate.decomposition(), randomOrders, candidateSeed(candidate));
    }

    private final class JoinOrderSelector {
        List<List<String>> sampleOrders(List<String> baseOrder, int randomOrders, long seed) {
            List<List<String>> orders = new java.util.ArrayList<>();
            List<String> frozenBase = List.copyOf(baseOrder);
            orders.add(frozenBase);
            if (randomOrders <= 0 || baseOrder.size() <= 1) {
                return orders;
            }
            java.util.Random random = new java.util.Random(seed);
            java.util.Set<List<String>> seen = new java.util.HashSet<>();
            seen.add(frozenBase);

            int attempts = 0;
            int targetSize = 1 + randomOrders;
            while (orders.size() < targetSize && attempts < randomOrders * 50) {
                List<String> shuffled = new java.util.ArrayList<>(baseOrder);
                java.util.Collections.shuffle(shuffled, random);
                if (seen.add(shuffled)) {
                    orders.add(shuffled);
                }
                attempts++;
            }
            return orders;
        }

        OrderEstimate estimateBestJoinOrder(
                Plan decomposition,
                List<Component> components,
                List<Long> resultCounts,
                List<Relation> relations,
                List<String> projected,
                int walks,
                int randomOrders,
                long seed,
                long deadlineNanos) {
            if (walks < 1) {
                throw new IllegalArgumentException("walks must be >= 1");
            }
            List<String> defaultOrder = orderByComponentCounts(components, resultCounts);
            if (defaultOrder.isEmpty()) {
                return new OrderEstimate(defaultOrder, 0.0, 0.0, 0.0);
            }

            List<String> estimateProjected = projected.isEmpty() ? defaultOrder : projected;
            long orderSeed = mixSeed(
                    seedForDecomposition(decomposition),
                    (int) seed,
                    (int) (seed >>> 32),
                    walks,
                    randomOrders);
            List<List<String>> orders = sampleOrders(defaultOrder, randomOrders, orderSeed);

            List<String> bestOrder = defaultOrder;
            double bestScore = Double.POSITIVE_INFINITY;
            int bestProjectionDepth = projectedDepth(defaultOrder, projected);
            double bestEstimatedCount = Double.NaN;
            double bestStandardError = Double.NaN;
            for (int i = 0; i < orders.size(); i++) {
                Deadline.check(deadlineNanos);
                List<String> order = orders.get(i);
                long walkSeed = mixSeed(orderSeed, i, order.hashCode(), walks);
                WanderJoinEstimator.Estimate estimate = estimateProjectedCount(
                        relations,
                        order,
                        estimateProjected,
                        walks,
                        walkSeed,
                        deadlineNanos);
                double score = conservativeEstimate(estimate.estimatedCount());
                int projectionDepth = projectedDepth(order, projected);
                if (score < bestScore
                        || (score == bestScore && projectionDepth < bestProjectionDepth)
                || (score == bestScore
                                && projectionDepth == bestProjectionDepth
                                && compareOrders(order, bestOrder) < 0)) {
                    bestScore = score;
                    bestProjectionDepth = projectionDepth;
                    bestOrder = order;
                    bestEstimatedCount = estimate.estimatedCount();
                    bestStandardError = estimate.standardError();
                }
            }
            return new OrderEstimate(bestOrder, bestScore, bestEstimatedCount, bestStandardError);
        }

        private int projectedDepth(List<String> order, List<String> projected) {
            if (projected.isEmpty()) {
                return order.size();
            }
            int depth = -1;
            for (String projectedVariable : projected) {
                int index = order.indexOf(projectedVariable);
                if (index > depth) {
                    depth = index;
                }
            }
            return depth < 0 ? order.size() : depth;
        }

        private int compareOrders(List<String> left, List<String> right) {
            int size = Math.min(left.size(), right.size());
            for (int i = 0; i < size; i++) {
                int cmp = left.get(i).compareTo(right.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(left.size(), right.size());
        }
    }

    private WanderJoinEstimator.Estimate estimateProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projected,
            int walks,
            long seed,
            long deadlineNanos) {
        return switch (config.estimatorType()) {
            case WANDERJOIN -> WanderJoinEstimator.estimateProjectedCount(
                    relations,
                    variableOrder,
                    projected,
                    walks,
                    seed,
                    config.wanderJoinRequireExtension(),
                    deadlineNanos);
            case SYSTEM_R -> estimateProjectedCountSystemR(relations, variableOrder, projected, deadlineNanos);
        };
    }

    private WanderJoinEstimator.Estimate estimateProjectedCountSystemR(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        if (relations.isEmpty()) {
            return new WanderJoinEstimator.Estimate(0.0, 0.0);
        }

        List<String> normalizedOrder = normalizeOrder(variableOrder, relations);
        java.util.Set<Relation> included = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        java.util.Set<String> bound = new java.util.HashSet<>();
        Map<String, Double> ndv = new HashMap<>();

        double outputCardinality = 1.0D;
        boolean initialized = false;
        for (String variable : normalizedOrder) {
            Deadline.check(deadlineNanos);
            bound.add(variable);
            for (Relation relation : relations) {
                Deadline.check(deadlineNanos);
                if (included.contains(relation)) {
                    continue;
                }
                List<String> variables = relation.variables();
                if (!bound.containsAll(variables)) {
                    continue;
                }

                double relationCardinality = normalizeCardinality(relation.tupleCount());
                if (!initialized) {
                    outputCardinality = relationCardinality;
                    initialized = true;
                } else {
                    double selectivity = 1.0D;
                    boolean joined = false;
                    for (String joinVar : variables) {
                        Double left = ndv.get(joinVar);
                        if (left == null || left <= 0.0D) {
                            continue;
                        }
                        double right = normalizeCardinality(relation.ndv(joinVar));
                        if (right <= 0.0D) {
                            continue;
                        }
                        joined = true;
                        selectivity /= Math.max(left, right);
                    }
                    outputCardinality *= relationCardinality;
                    if (joined) {
                        outputCardinality *= selectivity;
                    }
                    outputCardinality = normalizeCardinality(outputCardinality);
                }

                for (String relationVar : variables) {
                    double relationNdv = normalizeCardinality(relation.ndv(relationVar));
                    Double current = ndv.get(relationVar);
                    if (current == null) {
                        ndv.put(relationVar, Math.min(relationNdv, outputCardinality));
                    } else {
                        ndv.put(relationVar, Math.min(Math.min(current, relationNdv), outputCardinality));
                    }
                }
                included.add(relation);
            }
        }

        for (Relation relation : relations) {
            if (included.contains(relation)) {
                continue;
            }
            double relationCardinality = normalizeCardinality(relation.tupleCount());
            outputCardinality = normalizeCardinality(outputCardinality * relationCardinality);
            for (String variable : relation.variables()) {
                double relationNdv = normalizeCardinality(relation.ndv(variable));
                Double current = ndv.get(variable);
                if (current == null) {
                    ndv.put(variable, Math.min(relationNdv, outputCardinality));
                } else {
                    ndv.put(variable, Math.min(Math.min(current, relationNdv), outputCardinality));
                }
            }
        }

        if (!initialized) {
            return new WanderJoinEstimator.Estimate(0.0, 0.0);
        }

        double projectedCardinality = outputCardinality;
        if (!projected.isEmpty()) {
            double projectedNdvProduct = 1.0D;
            for (String variable : projected) {
                Double value = ndv.get(variable);
                if (value == null) {
                    continue;
                }
                projectedNdvProduct = safeMultiply(projectedNdvProduct, value);
            }
            projectedCardinality = Math.min(projectedCardinality, projectedNdvProduct);
        }
        return new WanderJoinEstimator.Estimate(normalizeCardinality(projectedCardinality), 0.0);
    }

    private static List<String> normalizeOrder(List<String> variableOrder, List<Relation> relations) {
        java.util.LinkedHashSet<String> variables = new java.util.LinkedHashSet<>(variableOrder);
        for (Relation relation : relations) {
            variables.addAll(relation.variables());
        }
        return List.copyOf(variables);
    }

    private static double safeMultiply(double left, double right) {
        if (!Double.isFinite(left) || !Double.isFinite(right)) {
            return Double.POSITIVE_INFINITY;
        }
        if (left <= 0.0D || right <= 0.0D) {
            return 0.0D;
        }
        if (left > Double.MAX_VALUE / right) {
            return Double.POSITIVE_INFINITY;
        }
        return left * right;
    }

    private static double normalizeCardinality(double cardinality) {
        if (!Double.isFinite(cardinality)) {
            return Double.POSITIVE_INFINITY;
        }
        return Math.max(1.0D, cardinality);
    }

    private double conservativeEstimate(double estimatedCount) {
        double clampedCount = Math.max(0.0, estimatedCount);
        return Double.isFinite(clampedCount) ? clampedCount : Double.POSITIVE_INFINITY;
    }

    private long seedForDecomposition(Plan decomposition) {
        long seed = mixSeed(config.estimationSeed(), decomposition.size(), decomposition.maxDiameter());
        for (Component component : decomposition.components()) {
            seed = mixSeed(
                    seed,
                    component.s().getName().hashCode(),
                    component.t().getName().hashCode(),
                    component.normalized().hashCode(),
                    component.mask().hashCode());
        }
        return seed;
    }

    private static long mixSeed(long seed, int... values) {
        long mixed = seed;
        for (int value : values) {
            mixed ^= value + 0x9E3779B97F4A7C15L + (mixed << 6) + (mixed >>> 2);
        }
        return mixed;
    }

    private static long candidateSeed(DecompositionCandidate candidate) {
        long method = candidate.method().ordinal();
        long ordinal = candidate.ordinal();
        return 0xC0FFEE ^ (method * 0x9E3779B97F4A7C15L) ^ (ordinal * 0xD6E8FEB86659FD93L);
    }

    private static List<String> orderByComponentCounts(List<Component> components, List<Long> resultCounts) {
        Map<String, Long> minCountByVar = new HashMap<>();
        Map<String, Integer> participationByVar = new HashMap<>();
        for (int i = 0; i < components.size(); i++) {
            long count = resultCounts.get(i);
            Component component = components.get(i);
            String left = component.sourceVarName();
            String right = component.targetVarName();
            minCountByVar.merge(left, count, Math::min);
            participationByVar.merge(left, 1, Integer::sum);
            if (!left.equals(right)) {
                minCountByVar.merge(right, count, Math::min);
                participationByVar.merge(right, 1, Integer::sum);
            }
        }
        return minCountByVar.keySet().stream()
                .sorted(Comparator
                        .comparingInt((String v) -> participationByVar.getOrDefault(v, 0))
                        .reversed()
                        .thenComparingLong((String v) -> minCountByVar.getOrDefault(v, Long.MAX_VALUE))
                        .thenComparing(Comparator.naturalOrder()))
                .toList();
    }
}
