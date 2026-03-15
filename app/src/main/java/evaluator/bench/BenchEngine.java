package evaluator.bench;

import static evaluator.bench.BenchTypes.*;

import evaluator.cpq.ConjunctiveQuery;
import evaluator.evaluation.Planner;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.cpq.Plan;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class BenchEngine {
    private static final int COMPARE_FILE_PROGRESS_EVERY = 25;
    private static final Planner.CandidateSelectionPolicy NATIVE_SELECTION =
            Planner.CandidateSelectionPolicy.METHOD_NATIVE;
    private static final Path COMPARE_FILE_WARMUP_SOURCE = Path.of(
            "local",
            "queries",
            "topology-bench",
            "workloads",
            "combination_queries",
            "viz",
            "queries.by_category.10_combinations.cq");
    private static final int COMPARE_FILE_WARMUP_QUERY_LIMIT = 100;

    private final CpqIndex index;
    private final Planner planner;
    private final EngineConfig config;
    private final EstimatorDiagnostics estimatorDiagnostics;

    BenchEngine(CpqIndex index) {
        this(index, EngineConfig.defaults());
    }

    BenchEngine(CpqIndex index, EngineConfig config) {
        this.index = Objects.requireNonNull(index, "index");
        this.config = Objects.requireNonNull(config, "config");
        if (index.k() < 1) {
            throw new IllegalArgumentException("index k must be >= 1");
        }
        this.planner = new Planner(index, config.estimationSeed(), config.systemRMaxCandidateOrders());
        this.estimatorDiagnostics = new EstimatorDiagnostics(planner, config);
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
        DecompositionMethod method = Objects.requireNonNull(spec.method(), "method");
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.kOverride(),
                spec.coverLimit(),
                spec.timeoutMs(),
                spec.timeoutMs(),
                index.k());
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
                    MethodDeadlines deadlines = MethodDeadlines.fromRunConfig(runConfig);
                    PreparedMethodSelection selection = selectPreparedCandidateForMethod(
                            cq,
                            method,
                            runConfig,
                            deadlines);
                    decomposeNanos = selection.decomposeNanos();
                    if (selection.candidate() == null) {
                        failures++;
                        if (selection.timedOutWithoutCandidate()) {
                            timeouts++;
                            status = BenchTypes.EvalFileStatus.TIMEOUT;
                        } else {
                            status = BenchTypes.EvalFileStatus.NO_DECOMPOSITIONS;
                        }
                        continue;
                    }
                    PreparedCandidate selected = selection.candidate();
                    methodId = selected.candidate().method().id();
                    components = selected.candidate().decomposition().size();
                    maxDiameter = selected.candidate().decomposition().maxDiameter();
                    EvaluationWithStats evaluation;
                    try {
                        evaluation = evaluateWithStats(selected.executable(), mode, deadlines.methodDeadlineNanos());
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
                    answers = BenchLogEmitter.answerCount(evaluation.result());
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

    private static List<DecompositionCandidate> toBenchSelectedCandidates(List<Planner.SelectedCandidate> candidates) {
        List<DecompositionCandidate> mapped = new ArrayList<>(candidates.size());
        for (Planner.SelectedCandidate candidate : candidates) {
            mapped.add(toBenchSelectedCandidate(candidate));
        }
        return List.copyOf(mapped);
    }

    private static DecompositionCandidate toBenchSelectedCandidate(Planner.SelectedCandidate candidate) {
        return new DecompositionCandidate(
                candidate.method(),
                candidate.ordinal(),
                candidate.plan(),
                candidate.decomposeNanos(),
                candidate.selectionEstimateNanos());
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
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.k(),
                spec.coverLimit(),
                spec.decompositionTimeoutMs(),
                spec.methodTimeoutMs(),
                index.k());
        ParsedQuery parsed = new ParsedQuery(cq, 0L, null);
        ComparisonPreparationStatus status = ComparisonPreparationStatus.NO_DECOMPOSITIONS;
        int completed = 0;
        int timeouts = 0;
        for (DecompositionMethod method : compareFileMethods()) {
            MethodOutcome<EvaluationWithStats> outcome = executeMethodRun(
                    parsed,
                    method,
                    runConfig,
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
                    if ("PLANNING_TIMEOUT".equals(outcome.status())) {
                        status = ComparisonPreparationStatus.READY;
                        timeouts++;
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
        Path warmupSource = spec.warmupQueriesFile() == null
                ? COMPARE_FILE_WARMUP_SOURCE
                : spec.warmupQueriesFile();
        int warmupLimit = spec.warmupQueryLimit() > 0
                ? spec.warmupQueryLimit()
                : COMPARE_FILE_WARMUP_QUERY_LIMIT;
        List<String> warmupQueries = spec.warmup()
                ? loadCompareFileWarmupQueries(warmupSource, warmupLimit)
                : List.of();
        EvaluationMode evaluationMode = spec.mode();
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
                    + (evaluationMode == EvaluationMode.ROWS ? " --rows" : " --count")
                    + " --per-method-timeout-ms " + methodTimeoutMs
                    + " --planning-timeout-ms " + decompositionTimeoutMs
                    + " --join-order-budget " + config.systemRMaxCandidateOrders()
                    + " --cover-limit " + coverLimit
                    + " --k " + k
                    + " --seed " + spec.seed();
            if (spec.warmup()) {
                command += " --warmup";
                if (spec.warmupQueriesFile() != null) {
                    command += " --warmup-queries-file " + spec.warmupQueriesFile();
                }
                if (spec.warmupQueryLimit() > 0) {
                    command += " --warmup-query-limit " + spec.warmupQueryLimit();
                }
            }
            BenchLogEmitter.printCompareFileHeader(
                    compareOut,
                    spec,
                    queries.size(),
                    warmupQueries.size(),
                    warmupSource.toString(),
                    warmupLimit,
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    config.systemRMaxCandidateOrders(),
                    coverLimit,
                    k,
                    command,
                    config.estimationSeed());
            if (decompositionOut != null) {
                decompositionOut.println("# " + command);
            }

            long warmupElapsedNanos = 0L;
            long warmupMethodRows = 0L;
            if (!warmupQueries.isEmpty()) {
                long warmupStartedNanos = System.nanoTime();
                warmupMethodRows = runCompareFileWarmup(
                        warmupQueries,
                        methods,
                        runConfig,
                        evaluationMode);
                warmupElapsedNanos = System.nanoTime() - warmupStartedNanos;
            }
            compareOut.println(String.format(
                    Locale.ROOT,
                    "warmup query_count=%d method_rows=%d elapsed_ms=%.3f",
                    warmupQueries.size(),
                    warmupMethodRows,
                    BenchLogEmitter.nanosToMillis(warmupElapsedNanos)));
            compareOut.flush();
            if (decompositionOut != null) {
                decompositionOut.flush();
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
                        runConfig,
                        // Compare-file logs counts only, but row-backed evaluation is a
                        // valid workload model when the caller wants timings that include
                        // distinct answer-set materialization without tuple printing.
                        (candidate, deadlineNanos) -> evaluateWithStats(
                                candidate.executable(),
                                evaluationMode,
                                deadlineNanos));

                for (MethodOutcome<EvaluationWithStats> outcome : outcomes) {
                    methodRows++;
                    DecompositionCandidate candidate = outcome.candidate();
                    if (candidate != null && decompositionOut != null) {
                        decompositionOut.println(BenchLogEmitter.formatDecompositionLine(queryNumber, candidate));
                    }
                    switch (outcome.type()) {
                        case PARSE_ERROR -> {
                            BenchLogEmitter.emitCompareFileRow(compareOut, BenchLogEmitter.CompareFileRow.parseError(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.wallNanos(),
                                    outcome.errorMessage()));
                            errorRows++;
                        }
                        case MISSING_CANDIDATE -> {
                            BenchLogEmitter.emitCompareFileRow(compareOut, BenchLogEmitter.CompareFileRow.withoutCandidate(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos(),
                                    outcome.status()));
                            if ("PLANNING_TIMEOUT".equals(outcome.status())) {
                                decompTimeoutRows++;
                            } else {
                                noCandidateRows++;
                            }
                        }
                        case TIMEOUT -> {
                            BenchLogEmitter.emitCompareFileRow(compareOut, BenchLogEmitter.CompareFileRow.timeout(
                                    queryNumber,
                                    outcome.method(),
                                    candidate,
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos()));
                            timeoutRows++;
                        }
                        case SUCCESS -> {
                            BenchLogEmitter.emitCompareFileRow(compareOut, BenchLogEmitter.CompareFileRow.ok(
                                    queryNumber,
                                    outcome.method(),
                                    candidate,
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
                    "summary query_count=%d method_rows=%d ok=%d exec_timeout=%d planning_timeout=%d no_candidate=%d error=%d elapsed_ms=%.3f",
                    queries.size(),
                    methodRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    BenchLogEmitter.nanosToMillis(elapsedNanos)));
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
                    + " --per-method-timeout-ms " + methodTimeoutMs
                    + " --planning-timeout-ms " + decompositionTimeoutMs
                    + " --join-order-budget " + config.systemRMaxCandidateOrders()
                    + " --cover-limit " + coverLimit
                    + " --k " + k
                    + " --seed " + spec.seed();
            if (spec.warmupQueriesFile() != null) {
                command += " --warmup-queries-file " + spec.warmupQueriesFile();
            }
            BenchLogEmitter.printEstimationBenchHeader(
                    out,
                    spec,
                    queries.size(),
                    warmupQueries.size(),
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    config.systemRMaxCandidateOrders(),
                    coverLimit,
                    k,
                    command,
                    spec.seed(),
                    config.estimationSeed());
            long warmupElapsedNanos = 0L;
            long warmupMethodRows = 0L;
            if (!warmupQueries.isEmpty()) {
                long warmupStartedNanos = System.nanoTime();
                warmupMethodRows = runEstimationBenchWarmup(
                        warmupQueries,
                        methods,
                        runConfig);
                warmupElapsedNanos = System.nanoTime() - warmupStartedNanos;
            }
            out.println(String.format(
                    Locale.ROOT,
                    "warmup query_count=%d method_rows=%d elapsed_ms=%.3f",
                    warmupQueries.size(),
                    warmupMethodRows,
                    BenchLogEmitter.nanosToMillis(warmupElapsedNanos)));
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
                        runConfig,
                        this::evaluateForEstimationBench);

                for (MethodOutcome<EstimationBenchEvaluation> outcome : outcomes) {
                    methodRows++;
                    DecompositionCandidate candidate = outcome.candidate();
                    switch (outcome.type()) {
                        case PARSE_ERROR -> {
                            stepRows++;
                            BenchLogEmitter.emitEstimationBenchRow(out, BenchLogEmitter.EstimationBenchRow.parseError(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.wallNanos(),
                                    outcome.errorMessage()));
                            errorRows++;
                        }
                        case MISSING_CANDIDATE -> {
                            stepRows++;
                            BenchLogEmitter.emitEstimationBenchRow(out, BenchLogEmitter.EstimationBenchRow.withoutCandidate(
                                    queryNumber,
                                    outcome.method(),
                                    outcome.parseNanos(),
                                    outcome.decomposeNanos(),
                                    outcome.wallNanos(),
                                    outcome.status()));
                            if ("PLANNING_TIMEOUT".equals(outcome.status())) {
                                decompTimeoutRows++;
                            } else {
                                noCandidateRows++;
                            }
                        }
                        case TIMEOUT -> {
                            stepRows++;
                            BenchLogEmitter.emitEstimationBenchRow(out, BenchLogEmitter.EstimationBenchRow.timeout(
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
                            List<EstimatorDiagnostics.PrefixEstimationStep> steps = outcome.evaluation().steps();
                            if (steps.isEmpty()) {
                                stepRows++;
                                BenchLogEmitter.emitEstimationBenchRow(out, BenchLogEmitter.EstimationBenchRow.okWithoutSteps(
                                        queryNumber,
                                        outcome.method(),
                                        candidate,
                                        evaluation,
                                        outcome.parseNanos(),
                                        outcome.wallNanos()));
                            } else {
                                for (EstimatorDiagnostics.PrefixEstimationStep step : steps) {
                                    stepRows++;
                                    BenchLogEmitter.emitEstimationBenchRow(out, BenchLogEmitter.EstimationBenchRow.okStep(
                                            queryNumber,
                                            outcome.method(),
                                            candidate,
                                            evaluation,
                                            step,
                                            outcome.parseNanos(),
                                            outcome.wallNanos()));
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
                    "summary query_count=%d method_rows=%d step_rows=%d ok=%d exec_timeout=%d planning_timeout=%d no_candidate=%d error=%d elapsed_ms=%.3f",
                    queries.size(),
                    methodRows,
                    stepRows,
                    okRows,
                    timeoutRows,
                    decompTimeoutRows,
                    noCandidateRows,
                    errorRows,
                    BenchLogEmitter.nanosToMillis(elapsedNanos)));
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

    private record MethodDeadlines(
            long startedNanos,
            long planningDeadlineNanos,
            long methodDeadlineNanos) {
        private static MethodDeadlines fromRunConfig(QueryRunConfig runConfig) {
            long startedNanos = System.nanoTime();
            long methodDeadlineNanos = deadlineFromStart(startedNanos, runConfig.methodTimeoutMs());
            long planningDeadlineNanos = runConfig.decompositionTimeoutMs() <= 0
                    ? methodDeadlineNanos
                    : Math.min(methodDeadlineNanos, deadlineFromStart(startedNanos, runConfig.decompositionTimeoutMs()));
            return new MethodDeadlines(startedNanos, planningDeadlineNanos, methodDeadlineNanos);
        }

        private long elapsedNanos() {
            return System.nanoTime() - startedNanos;
        }
    }

    private record MethodCandidateSelection(
            DecompositionCandidate candidate,
            boolean planningTimedOut,
            long planningNanos) {
        private String missingStatus() {
            return planningTimedOut ? "PLANNING_TIMEOUT" : "NO_CANDIDATE";
        }
    }

    private record PreparedMethodSelection(
            PreparedCandidate candidate,
            boolean timedOutWithoutCandidate,
            long decomposeNanos) {
        private String missingStatus() {
            return timedOutWithoutCandidate ? "PLANNING_TIMEOUT" : "NO_CANDIDATE";
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
            QueryRunConfig runConfig,
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
                    runConfig,
                    evaluator));
        }
        return List.copyOf(outcomes);
    }

    private <T> MethodOutcome<T> executeMethodRun(
            ParsedQuery parsed,
            DecompositionMethod method,
            QueryRunConfig runConfig,
            CandidateEvaluator<T> evaluator) {
        MethodDeadlines deadlines = MethodDeadlines.fromRunConfig(runConfig);
        MethodCandidateSelection selection = null;
        try {
            selection = selectCandidateForMethod(
                    parsed.cq(),
                    method,
                    runConfig,
                    deadlines.planningDeadlineNanos());
            if (selection.candidate() == null) {
                return new MethodOutcome<>(
                        MethodOutcomeType.MISSING_CANDIDATE,
                        method,
                        null,
                        parsed.parseNanos(),
                        selection.planningNanos(),
                        deadlines.elapsedNanos(),
                        selection.missingStatus(),
                        null,
                        null);
            }

            PreparedCandidate prepared = prepareCandidate(selection.candidate(), deadlines.methodDeadlineNanos());
            T evaluation = evaluator.evaluate(prepared, deadlines.methodDeadlineNanos());
            return new MethodOutcome<>(
                    MethodOutcomeType.SUCCESS,
                    method,
                    selection.candidate(),
                    parsed.parseNanos(),
                    selection.planningNanos(),
                    deadlines.elapsedNanos(),
                    "OK",
                    null,
                    evaluation);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new MethodOutcome<>(
                    MethodOutcomeType.TIMEOUT,
                    method,
                    selection == null ? null : selection.candidate(),
                    parsed.parseNanos(),
                    selection == null ? 0L : selection.planningNanos(),
                    deadlines.elapsedNanos(),
                    "EXEC_TIMEOUT",
                    null,
                    null);
        }
    }

    private MethodCandidateSelection selectCandidateForMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            QueryRunConfig runConfig,
            long planningDeadlineNanos) {
        long planningStartNanos = System.nanoTime();
        try {
            Planner.MethodSelection planned = planner.planMethod(
                    cq,
                    method,
                    runConfig.coverLimit(),
                    runConfig.k(),
                    planningDeadlineNanos,
                    planningDeadlineNanos);
            Planner.SelectedMethodSelection selected = planner.selectBestCandidate(
                    planned,
                    NATIVE_SELECTION,
                    planningDeadlineNanos);
            if (selected.candidate() == null) {
                return new MethodCandidateSelection(null, planned.timedOut(), planned.decomposeNanos());
            }
            return new MethodCandidateSelection(
                    toBenchSelectedCandidate(selected.candidate()),
                    planned.timedOut(),
                    planned.decomposeNanos());
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new MethodCandidateSelection(null, true, System.nanoTime() - planningStartNanos);
        }
    }

    private PreparedMethodSelection selectPreparedCandidateForMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            QueryRunConfig runConfig,
            MethodDeadlines deadlines) {
        MethodCandidateSelection selection = selectCandidateForMethod(
                cq,
                method,
                runConfig,
                deadlines.planningDeadlineNanos());
        if (selection.candidate() == null) {
            return new PreparedMethodSelection(null, selection.planningTimedOut(), selection.planningNanos());
        }
        PreparedCandidate prepared = prepareCandidate(selection.candidate(), deadlines.methodDeadlineNanos());
        return new PreparedMethodSelection(prepared, selection.planningTimedOut(), selection.planningNanos());
    }

    private PreparedCandidate requirePreparedCandidateForMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            QueryRunConfig runConfig,
            MethodDeadlines deadlines) {
        PreparedMethodSelection selection = selectPreparedCandidateForMethod(cq, method, runConfig, deadlines);
        if (selection.candidate() != null) {
            return selection.candidate();
        }
        if (selection.timedOutWithoutCandidate()) {
            throw new Deadline.Exceeded();
        }
        throw new IllegalStateException("No decomposition candidate for method " + method.id());
    }

    private long runCompareFileWarmup(
            List<String> warmupQueries,
            List<DecompositionMethod> methods,
            QueryRunConfig runConfig,
            EvaluationMode evaluationMode) {
        long warmedMethodRows = 0L;
        for (String queryText : warmupQueries) {
            ConjunctiveQuery cq;
            try {
                cq = parseCQ(queryText);
            } catch (Exception ignored) {
                continue;
            }
            ComparisonCandidates comparison;
            try {
                comparison = prepareComparisonCandidates(
                        cq,
                        runConfig.coverLimit(),
                        runConfig.k(),
                        runConfig.decompositionTimeoutMs(),
                        runConfig.methodTimeoutMs());
            } catch (RuntimeException ignored) {
                // Warmup is best-effort and should not fail the measured run.
                continue;
            }
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
                    long deadlineNanos = Deadline.afterMillis(runConfig.methodTimeoutMs());
                    PreparedCandidate prepared = prepareCandidate(candidate, deadlineNanos);
                    evaluateWithStats(prepared.executable(), evaluationMode, deadlineNanos);
                } catch (RuntimeException ignored) {
                    // Warmup is best-effort and should not fail the measured run.
                }
            }
        }
        return warmedMethodRows;
    }

    private long runEstimationBenchWarmup(
            List<String> warmupQueries,
            List<DecompositionMethod> methods,
            QueryRunConfig runConfig) {
        long warmedMethodRows = 0L;
        for (String queryText : warmupQueries) {
            ConjunctiveQuery cq;
            try {
                cq = parseCQ(queryText);
            } catch (Exception ignored) {
                continue;
            }
            ComparisonCandidates comparison;
            try {
                comparison = prepareComparisonCandidates(
                        cq,
                        runConfig.coverLimit(),
                        runConfig.k(),
                        runConfig.decompositionTimeoutMs(),
                        runConfig.methodTimeoutMs());
            } catch (RuntimeException ignored) {
                // Warmup is best-effort and should not fail the measured run.
                continue;
            }
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
                    long deadlineNanos = Deadline.afterMillis(runConfig.methodTimeoutMs());
                    PreparedCandidate prepared = prepareCandidate(candidate, deadlineNanos);
                    evaluateForEstimationBench(prepared, deadlineNanos);
                } catch (RuntimeException ignored) {
                    // Warmup is best-effort and should not fail the measured run.
                }
            }
        }
        return warmedMethodRows;
    }

    /**
     * Loads the fixed compare-file warmup workload and truncates it to the
     * first configured query rows after comment/blank filtering.
     */
    private static List<String> loadCompareFileWarmupQueries(Path warmupSource, int warmupQueryLimit) throws Exception {
        List<String> queries = loadQueries(warmupSource);
        int warmupQueryCount = Math.min(Math.max(0, warmupQueryLimit), queries.size());
        return List.copyOf(queries.subList(0, warmupQueryCount));
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

    private static long deadlineFromStart(long startedNanos, int timeoutMs) {
        if (timeoutMs <= 0) {
            return Long.MAX_VALUE;
        }
        long timeoutNanos = timeoutMs * 1_000_000L;
        long deadlineNanos = startedNanos + timeoutNanos;
        return deadlineNanos < 0L ? Long.MAX_VALUE : deadlineNanos;
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
     * Runs the profile workflow on one explicit decomposition method.
     */
    public BenchTypes.ProfileReport profile(BenchTypes.ProfileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.k(),
                spec.coverLimit(),
                spec.timeoutMs(),
                spec.timeoutMs(),
                index.k());
        try {
            MethodDeadlines deadlines = MethodDeadlines.fromRunConfig(runConfig);
            PreparedCandidate candidate = requirePreparedCandidateForMethod(
                    cq,
                    spec.method(),
                    runConfig,
                    deadlines);
            OrderProfileSummary summary = estimatorDiagnostics.profileOrders(
                    candidate.executable(),
                    Math.max(0, spec.profileOrders()),
                    spec.seed(),
                    deadlines.methodDeadlineNanos());
            return new BenchTypes.ProfileReport(summary.profiles().size(), false);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new BenchTypes.ProfileReport(0, true);
        }
    }

    /**
     * Runs the estimate workflow on one explicit decomposition method.
     */
    public BenchTypes.EstimateReport estimate(BenchTypes.EstimateSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        QueryRunConfig runConfig = normalizeRunConfig(
                spec.k(),
                spec.coverLimit(),
                spec.timeoutMs(),
                spec.timeoutMs(),
                index.k());
        try {
            MethodDeadlines deadlines = MethodDeadlines.fromRunConfig(runConfig);
            PreparedCandidate candidate = requirePreparedCandidateForMethod(
                    cq,
                    spec.method(),
                    runConfig,
                    deadlines);
            CardinalityEstimate estimate = estimatorDiagnostics.estimateCount(
                    candidate.executable(),
                    deadlines.methodDeadlineNanos());
            return new BenchTypes.EstimateReport(
                    estimate.estimatedCount(),
                    estimate.standardError(),
                    false);
        } catch (Deadline.Exceeded | java.util.concurrent.CancellationException ex) {
            return new BenchTypes.EstimateReport(Double.NaN, Double.NaN, true);
        }
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
     * Selects one indexable decomposition candidate per method by reranking
     * candidates with the shared projected-count scorer when enabled.
     *
     * @param cq Query to decompose.
     * @param coverLimit Maximum number of exact covers to consider per method.
     * @param k Maximum CPQ diameter to allow when decomposing.
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
        return selectBestCandidatesWithTimeoutInfo(
                cq,
                coverLimit,
                k,
                decompositionTimeoutMs,
                decompositionTimeoutMs);
    }

    CandidateSelection selectBestCandidatesWithTimeoutInfo(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs) {
        Planner.SelectedSelection selected = planner.selectBestCandidates(
                cq,
                coverLimit,
                k,
                decompositionTimeoutMs,
                methodTimeoutMs,
                NATIVE_SELECTION);
        return new CandidateSelection(
                toBenchSelectedCandidates(selected.candidates()),
                Set.copyOf(selected.timedOutMethods()),
                Map.copyOf(selected.decompositionNanosByMethod()));
    }

    private PreparedCandidateSelection selectBestPreparedCandidatesWithTimeoutInfo(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            long deadlineNanos) {
        Planner.Selection planned = planner.planAll(cq, coverLimit, k, decompositionTimeoutMs);
        Planner.SelectedSelection selected = planner.selectBestCandidates(
                planned,
                NATIVE_SELECTION,
                deadlineNanos);
        return new PreparedCandidateSelection(
                prepareCandidates(toBenchSelectedCandidates(selected.candidates()), deadlineNanos),
                Set.copyOf(selected.timedOutMethods()),
                Map.copyOf(selected.decompositionNanosByMethod()));
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
        return prepareComparisonCandidates(cq, coverLimit, k, decompositionTimeoutMs, decompositionTimeoutMs);
    }

    ComparisonCandidates prepareComparisonCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs) {
        CandidateSelection selection = selectBestCandidatesWithTimeoutInfo(
                cq,
                coverLimit,
                k,
                decompositionTimeoutMs,
                methodTimeoutMs);
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

    private List<PreparedCandidate> prepareCandidates(
            List<DecompositionCandidate> candidates,
            long deadlineNanos) {
        List<PreparedCandidate> prepared = new ArrayList<>(candidates.size());
        for (DecompositionCandidate candidate : candidates) {
            prepared.add(prepareCandidate(candidate, deadlineNanos));
        }
        return List.copyOf(prepared);
    }

    private record EstimationBenchEvaluation(
            EvaluationWithStats evaluation,
            List<EstimatorDiagnostics.PrefixEstimationStep> steps) {
    }

    private record PreparedCandidateSelection(
            List<PreparedCandidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
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

        Planner.JoinOrderPlan orderPlan = planner.selectJoinOrder(
                executable,
                false,
                deadlineNanos);
        stats.addEstimateNanos(orderPlan.estimateNanos());
        List<String> order = orderPlan.order();
        double estimatedCount = orderPlan.estimatedCount();
        double estimateStdError = orderPlan.estimateStdError();
        Deadline.check(deadlineNanos);
        long joinStart = System.nanoTime();
        EvaluationResult result = switch (mode) {
            case ROWS -> {
                LeapfrogJoin.JoinResult.Rows rows = (LeapfrogJoin.JoinResult.Rows) executable.join(
                        order,
                        LeapfrogJoin.JoinMode.PROJECTED_ROWS,
                        config.joinSafeDistinctFastPath(),
                        deadlineNanos);
                yield RowResult.fromRows(rows.rows(), deadlineNanos);
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
            long deadlineNanos) {
        EvaluationWithStats evaluation = evaluateWithStats(candidate.executable(), EvaluationMode.COUNT, deadlineNanos);
        if (evaluation.result() instanceof CountResult count && count.count() <= 0L) {
            return new EstimationBenchEvaluation(evaluation, List.of());
        }
        List<EstimatorDiagnostics.PrefixEstimationStep> steps = estimatorDiagnostics.tracePrefixEstimationSteps(
                candidate.executable(),
                evaluation.variableOrder(),
                deadlineNanos);
        return new EstimationBenchEvaluation(evaluation, steps);
    }

    CardinalityEstimate estimateCount(Plan decomposition, long deadlineNanos) {
        Objects.requireNonNull(decomposition, "decomposition");
        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index, deadlineNanos);
        return estimatorDiagnostics.estimateCount(executable, deadlineNanos);
    }

    OrderProfileSummary profileOrders(Plan decomposition, int randomOrders, long seed, long deadlineNanos) {
        Objects.requireNonNull(decomposition, "decomposition");
        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index, deadlineNanos);
        return estimatorDiagnostics.profileOrders(executable, randomOrders, seed, deadlineNanos);
    }

    private double conservativeEstimate(double estimatedCount) {
        double clampedCount = Math.max(0.0, estimatedCount);
        return Double.isFinite(clampedCount) ? clampedCount : Double.POSITIVE_INFINITY;
    }

}
