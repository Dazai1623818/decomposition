package evaluator.bench;

import static evaluator.bench.BenchTypes.*;

import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan.Component;
import evaluator.evaluation.Planner;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.WanderJoinEstimator;
import evaluator.cpq.Plan;
import evaluator.evaluation.Relation;
import evaluator.evaluation.ExecutablePlan;
import evaluator.index.CpqIndex;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        this.planner = new Planner(index);
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
        int coverLimit = spec.coverLimit() > 0 ? spec.coverLimit() : 1;
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
                    List<DecompositionCandidate> candidates = selectBestCandidates(cq, coverLimit, k, decompositionTimeoutMs);
                    decomposeNanos = System.nanoTime() - decomposeStart;
                    if (candidates.isEmpty()) {
                        failures++;
                        status = BenchTypes.EvalFileStatus.NO_DECOMPOSITIONS;
                        continue;
                    }
                    DecompositionCandidate selected = candidates.get(0);
                    methodId = toMethodId(selected.method());
                    components = selected.decomposition().size();
                    maxDiameter = selected.decomposition().maxDiameter();
                    TimedResult<EvaluationWithStats> timed = runWithOptionalTimeout(
                            () -> evaluateWithStats(selected.decomposition(), mode),
                            methodTimeoutMs,
                            "Interrupted while evaluating query");
                    if (timed.timedOut()) {
                        failures++;
                        timeouts++;
                        status = BenchTypes.EvalFileStatus.TIMEOUT;
                        continue;
                    }
                    EvaluationWithStats evaluation = timed.value();
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

    private static String toMethodId(DecompositionMethod method) {
        return method.id();
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
                toBenchMethod(candidate.method()),
                candidate.ordinal(),
                candidate.plan(),
                candidate.decomposeNanos());
    }

    private static Set<DecompositionMethod> toBenchMethods(Set<Planner.Method> methods) {
        EnumSet<DecompositionMethod> mapped = EnumSet.noneOf(DecompositionMethod.class);
        for (Planner.Method method : methods) {
            mapped.add(toBenchMethod(method));
        }
        return Set.copyOf(mapped);
    }

    private static DecompositionMethod toBenchMethod(Planner.Method method) {
        return switch (method) {
            case SINGLE_EDGE -> DecompositionMethod.SINGLE_EDGE;
            case COST -> DecompositionMethod.COST;
            case DIAMETER -> DecompositionMethod.DIAMETER;
            case SPQR -> DecompositionMethod.SPQR;
            case SERIES_PARALLEL -> DecompositionMethod.SERIES_PARALLEL;
        };
    }

    private Planner.PreparedPlan prepare(DecompositionCandidate candidate) {
        return planner.prepare(toPlannerCandidate(candidate));
    }

    private static Planner.Candidate toPlannerCandidate(DecompositionCandidate candidate) {
        return new Planner.Candidate(
                toPlannerMethod(candidate.method()),
                candidate.ordinal(),
                candidate.decomposition(),
                candidate.decomposeNanos());
    }

    private static Planner.Method toPlannerMethod(DecompositionMethod method) {
        return switch (method) {
            case SINGLE_EDGE -> Planner.Method.SINGLE_EDGE;
            case COST -> Planner.Method.COST;
            case DIAMETER -> Planner.Method.DIAMETER;
            case SPQR -> Planner.Method.SPQR;
            case SERIES_PARALLEL -> Planner.Method.SERIES_PARALLEL;
        };
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
                Math.max(1, spec.coverLimit()),
                spec.k() > 0 ? spec.k() : index.k(),
                Math.max(0, spec.decompositionTimeoutMs()),
                Math.max(0, spec.minComponents()),
                Math.max(0, spec.maxComponents()),
                Math.max(0, spec.candidateLimit()));
        return new BenchTypes.ExploreReport(candidates.selectedCandidates().size());
    }

    /**
     * Runs the compare workflow and returns compared method/candidate count.
     */
    public BenchTypes.CompareReport compare(BenchTypes.CompareSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        EvaluationMode mode = Objects.requireNonNull(spec.evaluationMode(), "evaluationMode");
        int methodTimeoutMs = Math.max(0, spec.methodTimeoutMs());
        ComparisonCandidates candidates = prepareComparisonCandidates(
                cq,
                Math.max(1, spec.coverLimit()),
                spec.k() > 0 ? spec.k() : index.k(),
                Math.max(0, spec.decompositionTimeoutMs()));
        int completed = 0;
        int timeouts = 0;
        for (DecompositionCandidate candidate : candidates.candidates()) {
            TimedResult<EvaluationResult> timed = runWithOptionalTimeout(
                    () -> evaluate(candidate.decomposition(), mode),
                    methodTimeoutMs,
                    "Interrupted while comparing candidates");
            if (timed.timedOut()) {
                timeouts++;
                continue;
            }
            completed++;
        }
        return new BenchTypes.CompareReport(completed, timeouts);
    }

    /**
     * Runs file-level compare output workflow.
     */
    public BenchTypes.CompareFileReport compareFile(BenchTypes.CompareFileSpec spec) throws Exception {
        Objects.requireNonNull(spec, "spec");
        List<String> queries = loadQueries(spec.queriesFile());
        int k = spec.kOverride() > 0 ? spec.kOverride() : index.k();
        int methodTimeoutMs = Math.max(0, spec.methodTimeoutMs());
        int decompositionTimeoutMs = Math.max(0, spec.decompositionTimeoutMs());
        int coverLimit = Math.max(1, spec.coverLimit());

        try (PrintWriter compareOut = openCompareWriter(spec.compareLogPath());
                PrintWriter decompositionOut = openDecompositionWriter(spec.decompositionLogPath())) {
            String command = "compare-file --index " + spec.indexPath()
                    + " --queries-file " + spec.queriesFile()
                    + " --method-timeout-ms " + methodTimeoutMs
                    + " --decomposition-timeout-ms " + decompositionTimeoutMs
                    + " --cover-limit " + coverLimit
                    + " --k " + k
                    + " --seed " + spec.seed();
            printCompareFileHeader(compareOut, spec, queries.size(), methodTimeoutMs, decompositionTimeoutMs, coverLimit, k, command);
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
                long parseStart = System.nanoTime();
                ConjunctiveQuery cq;
                try {
                    cq = parseCQ(queryText);
                } catch (Exception ex) {
                    long parseNanos = System.nanoTime() - parseStart;
                    for (DecompositionMethod method : DecompositionMethod.values()) {
                        emitCompareFileRow(
                                compareOut,
                                queryNumber,
                                method,
                                -1,
                                0,
                                0,
                                0,
                                -1L,
                                parseNanos,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                "ERROR",
                                summarizeError(ex));
                        methodRows++;
                        errorRows++;
                    }
                    continue;
                }
                long parseNanos = System.nanoTime() - parseStart;

                long selectStart = System.nanoTime();
                ComparisonCandidates comparison = prepareComparisonCandidates(
                        cq,
                        coverLimit,
                        k,
                        decompositionTimeoutMs);
                long selectionNanos = System.nanoTime() - selectStart;

                Map<DecompositionMethod, DecompositionCandidate> byMethod = new EnumMap<>(DecompositionMethod.class);
                for (DecompositionCandidate candidate : comparison.candidates()) {
                    byMethod.put(candidate.method(), candidate);
                }

                for (DecompositionMethod method : DecompositionMethod.values()) {
                    methodRows++;
                    DecompositionCandidate candidate = byMethod.get(method);
                    if (candidate == null) {
                        String status = comparison.timedOutWithoutCandidate().contains(method)
                                ? "DECOMP_TIMEOUT"
                                : "NO_CANDIDATE";
                        emitCompareFileRow(
                                compareOut,
                                queryNumber,
                                method,
                                -1,
                                0,
                                0,
                                0,
                                -1L,
                                parseNanos,
                                selectionNanos,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                status,
                                null);
                        if ("DECOMP_TIMEOUT".equals(status)) {
                            decompTimeoutRows++;
                        } else {
                            noCandidateRows++;
                        }
                        continue;
                    }

                    if (decompositionOut != null) {
                        decompositionOut.println(formatDecompositionLine(queryNumber, candidate));
                    }

                    long evalStart = System.nanoTime();
                    TimedResult<EvaluationWithStats> timed = runWithOptionalTimeout(
                            () -> evaluateWithStats(candidate.decomposition(), EvaluationMode.COUNT),
                            methodTimeoutMs,
                            "Interrupted while evaluating method");
                    long wallNanos = System.nanoTime() - evalStart;
                    int edgesCollapsed = edgesCollapsed(candidate.decomposition());
                    if (timed.timedOut()) {
                        emitCompareFileRow(
                                compareOut,
                                queryNumber,
                                method,
                                candidate.ordinal(),
                                candidate.decomposition().size(),
                                candidate.decomposition().maxDiameter(),
                                edgesCollapsed,
                                -1L,
                                parseNanos,
                                candidate.decomposeNanos(),
                                candidate.selectionEstimateNanos(),
                                wallNanos,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                "TIMEOUT",
                                null);
                        timeoutRows++;
                        continue;
                    }

                    EvaluationWithStats evaluation = timed.value();
                    long answers = answerCount(evaluation.result());
                    long queryNanos = evaluation.stats().queryNanos();
                    long mappingNanos = evaluation.stats().mappingNanos();
                    long estimateNanos = evaluation.stats().estimateNanos();
                    long joinNanos = evaluation.stats().joinNanos();
                    emitCompareFileRow(
                            compareOut,
                            queryNumber,
                            method,
                            candidate.ordinal(),
                            candidate.decomposition().size(),
                            candidate.decomposition().maxDiameter(),
                            edgesCollapsed,
                            answers,
                            parseNanos,
                            candidate.decomposeNanos(),
                            candidate.selectionEstimateNanos(),
                            wallNanos,
                            queryNanos + mappingNanos + estimateNanos + joinNanos,
                            queryNanos,
                            mappingNanos,
                            estimateNanos,
                            joinNanos,
                            "OK",
                            null);
                    okRows++;
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
        }
    }

    private static void printCompareFileHeader(
            PrintWriter compareOut,
            BenchTypes.CompareFileSpec spec,
            int totalQueries,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int k,
            String command) {
        compareOut.println("started=" + Instant.now());
        compareOut.println("index=" + spec.indexPath());
        compareOut.println("queries=" + spec.queriesFile());
        compareOut.println("total_queries=" + totalQueries);
        compareOut.println("method_timeout_ms=" + methodTimeoutMs);
        compareOut.println("decomposition_timeout_ms=" + decompositionTimeoutMs);
        compareOut.println("cover_limit=" + coverLimit);
        compareOut.println("k=" + k);
        compareOut.println("seed=" + spec.seed());
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

    private static void emitCompareFileRow(
            PrintWriter out,
            int queryNumber,
            DecompositionMethod method,
            int ordinal,
            int components,
            int maxDiameter,
            int edgesCollapsed,
            long answers,
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
        String errorSegment = errorMessage == null || errorMessage.isBlank()
                ? ""
                : String.format(Locale.ROOT, " error=\"%s\"", sanitize(errorMessage));
        out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d comps=%d max_diam=%d edges_collapsed=%d answers=%d parse_ms=%.3f decompose_ms=%.3f decomp_estimate_ms=%.3f wall_ms=%.3f total_ms=%.3f query_ms=%.3f mapping_ms=%.3f estimate_ms=%.3f join_ms=%.3f status=%s%s",
                queryNumber,
                method.name(),
                ordinal,
                components,
                maxDiameter,
                edgesCollapsed,
                answers,
                nanosToMillis(parseNanos),
                nanosToMillis(decomposeNanos),
                nanosToMillis(decompEstimateNanos),
                nanosToMillis(wallNanos),
                nanosToMillis(totalNanos),
                nanosToMillis(queryNanos),
                nanosToMillis(mappingNanos),
                nanosToMillis(estimateNanos),
                nanosToMillis(joinNanos),
                status,
                errorSegment));
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

    /**
     * Runs the profile workflow on the default decomposition.
     */
    public BenchTypes.ProfileReport profile(BenchTypes.ProfileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        Plan decomposition = decompose(cq);
        TimedResult<OrderProfileSummary> timed = runWithOptionalTimeout(
                () -> profileOrders(
                        decomposition,
                        Math.max(0, spec.profileOrders()),
                        spec.seed()),
                Math.max(0, spec.profileTimeoutMs()),
                "Interrupted while profiling join orders");
        if (timed.timedOut()) {
            return new BenchTypes.ProfileReport(0, true);
        }
        return new BenchTypes.ProfileReport(timed.value().profiles().size(), false);
    }

    /**
     * Runs the estimate workflow on the default decomposition.
     */
    public BenchTypes.EstimateReport estimate(BenchTypes.EstimateSpec spec) {
        Objects.requireNonNull(spec, "spec");
        ConjunctiveQuery cq = parseCQ(spec.queryText());
        Plan decomposition = decompose(cq);
        TimedResult<CardinalityEstimate> timed = runWithOptionalTimeout(
                () -> estimateCount(
                        decomposition,
                        Math.max(1, spec.walks()),
                        spec.seed()),
                Math.max(0, spec.methodTimeoutMs()),
                "Interrupted while estimating cardinality");
        if (timed.timedOut()) {
            return new BenchTypes.EstimateReport(Double.NaN, Double.NaN, true);
        }
        return new BenchTypes.EstimateReport(
                timed.value().estimatedCount(),
                timed.value().standardError(),
                false);
    }

    private static <T> TimedResult<T> runWithOptionalTimeout(
            Callable<T> task,
            int timeoutMs,
            String interruptMessage) {
        Objects.requireNonNull(task, "task");
        if (timeoutMs <= 0) {
            try {
                return new TimedResult<>(task.call(), false);
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<T> future = executor.submit(task);
            try {
                return new TimedResult<>(future.get(timeoutMs, TimeUnit.MILLISECONDS), false);
            } catch (TimeoutException ex) {
                future.cancel(true);
                return new TimedResult<>(null, true);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interruptMessage, ex);
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new RuntimeException(cause);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private record TimedResult<T>(T value, boolean timedOut) {
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
     * heuristic generator order, prioritizing fewer joins and then running a
     * staged Wander Join search over decomposition and join-order pairs.
     *
     * @param cq         Query to decompose.
     * @param coverLimit Maximum number of exact covers to consider per method.
     * @return Selected candidates, one per method when available.
     */
    List<DecompositionCandidate> selectBestCandidates(ConjunctiveQuery cq, int coverLimit) {
        return selectBestCandidates(cq, coverLimit, index.k(), 0);
    }

    /**
     * Selects one indexable decomposition candidate per method using the
     * heuristic generator order, prioritizing fewer joins and then running a
     * staged Wander Join search over decomposition and join-order pairs.
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
        return new CandidateSelection(selected, toBenchMethods(planned.timedOutMethods()));
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
                Set.copyOf(timedOutWithoutCandidate));
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
        List<DecompositionCandidate> selected = selectBestCandidates(cq, coverLimit, k, decompositionTimeoutMs);
        List<DecompositionEvaluation> evaluations = new java.util.ArrayList<>(selected.size());
        for (DecompositionCandidate candidate : selected) {
            EvaluationWithStats evaluation = evaluateWithStats(candidate.decomposition(), mode);
            evaluations.add(new DecompositionEvaluation(
                    candidate.method(),
                    candidate.ordinal(),
                    candidate.decomposition(),
                    candidate.decomposeNanos(),
                    evaluation));
        }
        return evaluations;
    }

    private record CandidateContext(
            DecompositionCandidate candidate,
            ExecutablePlan executable,
            List<String> projected) {
    }

    private record CandidateEstimate(double conservativeCount, long prepNanos) {
    }

    private record ScoredCandidate(CandidateContext context, CandidateEstimate estimate) {
    }

    private record OrderEstimate(List<String> order, double conservativeCount) {
    }

    private record HubPreference(String variable, double penalty) {
    }

    private List<DecompositionCandidate> pickBestByMethodWithEstimation(List<DecompositionCandidate> candidates) {
        Map<DecompositionMethod, List<DecompositionCandidate>> byMethod = new EnumMap<>(DecompositionMethod.class);
        for (DecompositionCandidate candidate : candidates) {
            byMethod.computeIfAbsent(candidate.method(), ignored -> new java.util.ArrayList<>()).add(candidate);
        }

        List<DecompositionCandidate> selected = new java.util.ArrayList<>(byMethod.size());
        for (List<DecompositionCandidate> methodCandidates : byMethod.values()) {
            selected.add(pickBestCandidateByEstimation(methodCandidates));
        }
        return selected;
    }

    private DecompositionCandidate pickBestDefaultCandidate(List<DecompositionCandidate> candidates) {
        int minComponents = Integer.MAX_VALUE;
        for (DecompositionCandidate candidate : candidates) {
            minComponents = Math.min(minComponents, candidate.decomposition().size());
        }

        List<DecompositionCandidate> fewest = new java.util.ArrayList<>();
        for (DecompositionCandidate candidate : candidates) {
            if (candidate.decomposition().size() == minComponents) {
                fewest.add(candidate);
            }
        }
        fewest.sort(this::compareCandidatesByHeuristic);
        if (fewest.size() <= 1) {
            return fewest.get(0);
        }

        List<CandidateContext> contexts = new java.util.ArrayList<>(fewest.size());
        for (DecompositionCandidate candidate : fewest) {
            Planner.PreparedPlan prepared = prepare(candidate);
            List<String> projected = prepared.plan().projectedVariableNames();
            contexts.add(new CandidateContext(candidate, prepared.executable(), projected));
        }

        DecompositionCandidate best = null;
        CandidateEstimate bestEstimate = null;
        for (int i = 0; i < contexts.size(); i++) {
            CandidateContext context = contexts.get(i);
            CandidateEstimate estimate = estimateCandidate(
                    context,
                    config.defaultDecomposeSelectionWalks(),
                    config.defaultDecomposeSelectionRandomOrders(),
                    config.defaultDecomposeSelectionStructuredOrders(),
                    3,
                    i);
            if (best == null) {
                best = context.candidate();
                bestEstimate = estimate;
                continue;
            }

            int cmp = compareCandidateEstimate(estimate, bestEstimate);
            if (cmp < 0
                    || (cmp == 0 && compareCandidatesByHeuristic(context.candidate(), best) < 0)) {
                best = context.candidate();
                bestEstimate = estimate;
            }
        }
        return best;
    }

    private DecompositionCandidate pickBestCandidateByEstimation(List<DecompositionCandidate> methodCandidates) {
        int minComponents = Integer.MAX_VALUE;
        for (DecompositionCandidate candidate : methodCandidates) {
            minComponents = Math.min(minComponents, candidate.decomposition().size());
        }

        List<DecompositionCandidate> fewestJoinCandidates = new java.util.ArrayList<>();
        for (DecompositionCandidate candidate : methodCandidates) {
            if (candidate.decomposition().size() == minComponents) {
                fewestJoinCandidates.add(candidate);
            }
        }
        fewestJoinCandidates.sort(this::compareCandidatesByHeuristic);

        int heuristicPoolSize = Math.min(fewestJoinCandidates.size(), config.decompositionEstimationTopK());
        List<DecompositionCandidate> heuristicPool = fewestJoinCandidates.subList(0, heuristicPoolSize);
        if (minComponents < config.decompositionEstimationMinComponents()) {
            return heuristicPool.get(0).withSelectionEstimateNanos(0L);
        }
        if (heuristicPoolSize <= 1) {
            return heuristicPool.get(0).withSelectionEstimateNanos(0L);
        }

        long selectionEstimateStart = System.nanoTime();
        List<CandidateContext> contexts = new java.util.ArrayList<>(heuristicPoolSize);
        for (DecompositionCandidate candidate : heuristicPool) {
            Planner.PreparedPlan prepared = prepare(candidate);
            List<String> projected = prepared.plan().projectedVariableNames();
            contexts.add(new CandidateContext(candidate, prepared.executable(), projected));
        }

        List<ScoredCandidate> stageOne = scoreCandidates(
                contexts,
                config.decompositionEstimationStage1Walks(),
                config.decompositionEstimationStage1RandomOrders(),
                config.decompositionEstimationStage1StructuredOrders(),
                1);
        stageOne.sort(this::compareScoredCandidates);

        int stageTwoTopK = Math.min(config.decompositionEstimationStage2TopK(), stageOne.size());
        ScoredCandidate best = null;
        for (int i = 0; i < stageTwoTopK; i++) {
            CandidateContext context = stageOne.get(i).context();
            CandidateEstimate stageTwoEstimate = estimateCandidate(
                    context,
                    config.decompositionEstimationStage2Walks(),
                    config.decompositionEstimationStage2RandomOrders(),
                    config.decompositionEstimationStage2StructuredOrders(),
                    2,
                    i);
            ScoredCandidate rescored = new ScoredCandidate(context, stageTwoEstimate);
            if (best == null || compareScoredCandidates(rescored, best) < 0) {
                best = rescored;
            }
        }
        DecompositionCandidate selected = (best == null ? stageOne.get(0) : best).context().candidate();
        long selectionEstimateNanos = System.nanoTime() - selectionEstimateStart;
        return selected.withSelectionEstimateNanos(selectionEstimateNanos);
    }

    private List<ScoredCandidate> scoreCandidates(
            List<CandidateContext> contexts,
            int walks,
            int randomOrders,
            int structuredOrders,
            int phase) {
        List<ScoredCandidate> scored = new java.util.ArrayList<>(contexts.size());
        for (int i = 0; i < contexts.size(); i++) {
            CandidateContext context = contexts.get(i);
            CandidateEstimate estimate = estimateCandidate(context, walks, randomOrders, structuredOrders, phase, i);
            scored.add(new ScoredCandidate(context, estimate));
        }
        return scored;
    }

    private CandidateEstimate estimateCandidate(
            CandidateContext context,
            int walks,
            int randomOrders,
            int structuredOrders,
            int phase,
            int position) {
        long prepNanos = context.executable().compilationStats().totalNanos();
        if (context.executable().isEmpty()) {
            return new CandidateEstimate(0.0, prepNanos);
        }

        long seed = mixSeed(
                config.estimationSeed(),
                context.candidate().method().ordinal(),
                context.candidate().ordinal(),
                phase,
                position);
        OrderEstimate orderEstimate = joinOrderSelector.estimateBestJoinOrder(
                context.candidate().decomposition(),
                context.executable().plan().components(),
                context.executable().componentCounts(),
                context.executable().relations(),
                context.projected(),
                walks,
                randomOrders,
                structuredOrders,
                seed);
        return new CandidateEstimate(orderEstimate.conservativeCount(), prepNanos);
    }

    private int compareScoredCandidates(ScoredCandidate left, ScoredCandidate right) {
        int cmp = compareCandidateEstimate(left.estimate(), right.estimate());
        if (cmp != 0) {
            return cmp;
        }
        return compareCandidatesByHeuristic(left.context().candidate(), right.context().candidate());
    }

    private static int compareCandidateEstimate(CandidateEstimate left, CandidateEstimate right) {
        int cmp = Double.compare(left.conservativeCount(), right.conservativeCount());
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(left.prepNanos(), right.prepNanos());
    }

    private int compareCandidatesByHeuristic(DecompositionCandidate left, DecompositionCandidate right) {
        int cmp = Integer.compare(left.decomposition().size(), right.decomposition().size());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(right.decomposition().maxDiameter(), left.decomposition().maxDiameter());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(maxComponentSize(right.decomposition()), maxComponentSize(left.decomposition()));
        if (cmp != 0) {
            return cmp;
        }
        cmp = Long.compare(totalCost(left.decomposition()), totalCost(right.decomposition()));
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.method().ordinal(), right.method().ordinal());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(left.ordinal(), right.ordinal());
    }

    private long totalCost(Plan decomposition) {
        long total = 0L;
        for (Component component : decomposition.components()) {
            total += index.cost(component.cpq());
        }
        return total;
    }

    private static int maxComponentSize(Plan decomposition) {
        int max = 0;
        for (Component component : decomposition.components()) {
            max = Math.max(max, component.mask().cardinality());
        }
        return max;
    }

    private static EvaluationResult emptyResult(EvaluationMode mode) {
        return mode == EvaluationMode.ROWS ? new RowResult(List.of()) : new CountResult(0L);
    }

    private static EvaluationWithStats emptyWithStats(EvaluationMode mode, EvaluationStats stats) {
        return new EvaluationWithStats(emptyResult(mode), stats);
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
        Objects.requireNonNull(decomposition, "decomposition");
        Objects.requireNonNull(mode, "mode");

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index);
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return emptyWithStats(mode, stats);
        }

        List<Component> components = executable.plan().components();
        List<Long> resultCounts = executable.componentCounts();
        List<Relation> relations = executable.relations();
        List<String> projected = decomposition.projectedVariableNames();
        int orderWalks = config.orderEstimationWalks();
        int randomOrders = config.orderEstimationRandomOrders();
        int structuredOrders = config.orderEstimationStructuredOrders();
        if (decomposition.size() >= config.orderEstimationHardMinComponents()) {
            orderWalks = config.orderEstimationHardWalks();
            randomOrders = config.orderEstimationHardRandomOrders();
            structuredOrders = config.orderEstimationHardStructuredOrders();
        }
        long estimateStart = System.nanoTime();
        OrderEstimate orderEstimate = joinOrderSelector.estimateBestJoinOrder(
                decomposition,
                components,
                resultCounts,
                relations,
                projected,
                orderWalks,
                randomOrders,
                structuredOrders,
                config.estimationSeed());
        stats.addEstimateNanos(System.nanoTime() - estimateStart);
        List<String> order = orderEstimate.order();
        long joinStart = System.nanoTime();
        EvaluationResult result = switch (mode) {
            case ROWS -> {
                LeapfrogJoin.JoinResult.Rows rows = (LeapfrogJoin.JoinResult.Rows) executable.join(
                        order,
                        LeapfrogJoin.JoinMode.PROJECTED_ROWS,
                        config.joinSafeDistinctFastPath());
                yield new RowResult(rows.rows());
            }
            case COUNT -> {
                LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                        order,
                        LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                        config.joinSafeDistinctFastPath());
                yield new CountResult(count.count());
            }
        };
        stats.addJoinNanos(System.nanoTime() - joinStart);
        return new EvaluationWithStats(result, stats);
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
     * Estimates projected answer cardinality via Wander Join random walks.
     *
     * @param decomposition Decomposition to estimate.
     * @param walks         Number of random walks to run.
     * @param seed          Random seed for reproducibility.
     * @return Estimate with standard error and phase timings.
     */
    CardinalityEstimate estimateCount(Plan decomposition, int walks, long seed) {
        Objects.requireNonNull(decomposition, "decomposition");
        if (walks < 1) {
            throw new IllegalArgumentException("walks must be >= 1");
        }

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index);
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return new CardinalityEstimate(0.0, 0.0, walks, seed, stats.queryNanos(), stats.mappingNanos(), 0L);
        }

        List<Component> components = executable.plan().components();
        List<Long> resultCounts = executable.componentCounts();
        List<String> order = orderByComponentCounts(components, resultCounts);
        List<String> projected = decomposition.projectedVariableNames();

        long estimateStart = System.nanoTime();
        WanderJoinEstimator.Estimate estimate = executable.estimateProjectedCount(order, projected, walks, seed);
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
        Objects.requireNonNull(decomposition, "decomposition");
        if (randomOrders < 0) {
            throw new IllegalArgumentException("randomOrders must be >= 0");
        }

        ExecutablePlan executable = ExecutablePlan.compile(decomposition, index);
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
            long start = System.nanoTime();
            LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                    order,
                    LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                    config.joinSafeDistinctFastPath());
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
            orders.add(List.copyOf(baseOrder));
            if (randomOrders == 0 || baseOrder.size() <= 1) {
                return orders;
            }
            java.util.Random random = new java.util.Random(seed);
            java.util.Set<List<String>> seen = new java.util.HashSet<>();
            seen.add(orders.get(0));

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
                int structuredOrders,
                long seed) {
            if (walks < 1) {
                throw new IllegalArgumentException("walks must be >= 1");
            }
            List<String> defaultOrder = orderByComponentCounts(components, resultCounts);
            if (defaultOrder.isEmpty()) {
                return new OrderEstimate(defaultOrder, 0.0);
            }

            List<String> estimateProjected = projected.isEmpty() ? defaultOrder : projected;
            HubPreference hubPreference = singleProjectedHubPreference(defaultOrder, projected, relations);
            long orderSeed = mixSeed(
                    seedForDecomposition(decomposition),
                    (int) seed,
                    (int) (seed >>> 32),
                    walks,
                    randomOrders);
            List<List<String>> sampled = candidateOrdersForEstimation(
                    defaultOrder,
                    relations,
                    structuredOrders,
                    randomOrders,
                    orderSeed);
            List<List<String>> orders = new java.util.ArrayList<>(sampled);
            java.util.Set<List<String>> seenOrders = new java.util.HashSet<>(orders);
            if (config.orderEstimationProjectedPrefixCandidate() && projected.size() > 1) {
                List<String> projectedPrefix = orderWithProjectedPrefix(defaultOrder, projected);
                if (seenOrders.add(projectedPrefix)) {
                    orders.add(projectedPrefix);
                }
            }
            if (hubPreference != null) {
                List<String> hubFirst = orderWithPinnedFirst(defaultOrder, hubPreference.variable());
                if (seenOrders.add(hubFirst)) {
                    orders.add(hubFirst);
                }
            }

            List<String> bestOrder = defaultOrder;
            double bestScore = Double.POSITIVE_INFINITY;
            for (int i = 0; i < orders.size(); i++) {
                List<String> order = orders.get(i);
                long walkSeed = mixSeed(orderSeed, i, order.hashCode(), walks);
                WanderJoinEstimator.Estimate estimate = WanderJoinEstimator.estimateProjectedCount(
                        relations,
                        order,
                        estimateProjected,
                        walks,
                        walkSeed);
                double score = conservativeEstimate(estimate.estimatedCount(), estimate.standardError());
                if (hubPreference != null && !hubPreference.variable().equals(order.get(0))) {
                    score *= hubPreference.penalty();
                }
                if (score < bestScore) {
                    bestScore = score;
                    bestOrder = order;
                }
            }
            return new OrderEstimate(bestOrder, bestScore);
        }

        private HubPreference singleProjectedHubPreference(
                List<String> baseOrder,
                List<String> projected,
                List<Relation> relations) {
            if (!config.orderEstimationSingleProjectedHubFirst() || projected.size() != 1 || baseOrder.size() <= 1) {
                return null;
            }
            String projectedVariable = projected.get(0);
            if (!baseOrder.contains(projectedVariable)) {
                return null;
            }
            Map<String, Integer> degreeByVar = variableDegrees(baseOrder, relations);
            int degree = degreeByVar.getOrDefault(projectedVariable, 0);
            if (degree < config.orderEstimationSingleProjectedHubMinDegree()) {
                return null;
            }
            return new HubPreference(projectedVariable, config.orderEstimationSingleProjectedHubPenalty());
        }

        private List<String> orderWithPinnedFirst(List<String> baseOrder, String firstVariable) {
            if (baseOrder.isEmpty() || firstVariable.equals(baseOrder.get(0))) {
                return baseOrder;
            }
            List<String> order = new java.util.ArrayList<>(baseOrder.size());
            order.add(firstVariable);
            for (String variable : baseOrder) {
                if (!firstVariable.equals(variable)) {
                    order.add(variable);
                }
            }
            return order;
        }

        private List<String> orderWithProjectedPrefix(List<String> baseOrder, List<String> projected) {
            if (baseOrder.isEmpty() || projected.isEmpty() || projected.size() >= baseOrder.size()) {
                return baseOrder;
            }
            Set<String> projectedSet = Set.copyOf(projected);
            List<String> order = new java.util.ArrayList<>(baseOrder.size());
            for (String variable : baseOrder) {
                if (projectedSet.contains(variable)) {
                    order.add(variable);
                }
            }
            if (order.size() <= 1 || order.size() == baseOrder.size()) {
                return baseOrder;
            }
            for (String variable : baseOrder) {
                if (!projectedSet.contains(variable)) {
                    order.add(variable);
                }
            }
            return order;
        }

        private List<List<String>> candidateOrdersForEstimation(
                List<String> baseOrder,
                List<Relation> relations,
                int structuredOrders,
                int randomOrders,
                long seed) {
            List<List<String>> orders = new java.util.ArrayList<>();
            java.util.Set<List<String>> seen = new java.util.HashSet<>();
            addOrderCandidate(orders, seen, baseOrder);
            if (baseOrder.size() <= 1) {
                return orders;
            }

            Map<String, Integer> position = new HashMap<>();
            for (int i = 0; i < baseOrder.size(); i++) {
                position.put(baseOrder.get(i), i);
            }
            Map<String, Integer> degreeByVar = variableDegrees(baseOrder, relations);

            int remainingStructured = structuredOrders;
            if (remainingStructured > 0) {
                addOrderCandidate(orders, seen, connectedOrder(baseOrder, degreeByVar, relations, position));
                remainingStructured--;
            }
            if (remainingStructured > 0) {
                addOrderCandidate(orders, seen, orderByDegree(baseOrder, degreeByVar, position, true));
                remainingStructured--;
            }
            if (remainingStructured > 0) {
                List<String> reversed = new java.util.ArrayList<>(baseOrder);
                java.util.Collections.reverse(reversed);
                addOrderCandidate(orders, seen, reversed);
                remainingStructured--;
            }
            if (remainingStructured > 0) {
                addOrderCandidate(orders, seen, orderByDegree(baseOrder, degreeByVar, position, false));
            }

            if (randomOrders == 0) {
                return orders;
            }

            java.util.Random random = new java.util.Random(seed);
            int attempts = 0;
            int targetSize = orders.size() + randomOrders;
            while (orders.size() < targetSize && attempts < randomOrders * 50) {
                List<String> shuffled = new java.util.ArrayList<>(baseOrder);
                java.util.Collections.shuffle(shuffled, random);
                addOrderCandidate(orders, seen, shuffled);
                attempts++;
            }
            return orders;
        }

        private void addOrderCandidate(
                List<List<String>> orders,
                Set<List<String>> seen,
                List<String> order) {
            List<String> frozen = List.copyOf(order);
            if (seen.add(frozen)) {
                orders.add(frozen);
            }
        }

        private Map<String, Integer> variableDegrees(
                List<String> order,
                List<Relation> relations) {
            Map<String, Integer> degreeByVar = new HashMap<>();
            for (String variable : order) {
                degreeByVar.put(variable, 0);
            }
            for (Relation relation : relations) {
                for (String variable : relation.variables()) {
                    degreeByVar.merge(variable, 1, Integer::sum);
                }
            }
            return degreeByVar;
        }

        private List<String> orderByDegree(
                List<String> baseOrder,
                Map<String, Integer> degreeByVar,
                Map<String, Integer> position,
                boolean descending) {
            List<String> ordered = new java.util.ArrayList<>(baseOrder);
            Comparator<String> byDegree = Comparator.comparingInt((String v) -> degreeByVar.getOrDefault(v, 0));
            if (descending) {
                byDegree = byDegree.reversed();
            }
            ordered.sort(byDegree.thenComparingInt(v -> position.getOrDefault(v, Integer.MAX_VALUE)));
            return ordered;
        }

        private List<String> connectedOrder(
                List<String> baseOrder,
                Map<String, Integer> degreeByVar,
                List<Relation> relations,
                Map<String, Integer> position) {
            Map<String, Set<String>> neighbors = new HashMap<>();
            for (String variable : baseOrder) {
                neighbors.put(variable, new java.util.HashSet<>());
            }
            for (Relation relation : relations) {
                String left = relation.sourceVar();
                String right = relation.targetVar();
                if (right != null && !left.equals(right)) {
                    neighbors.computeIfAbsent(left, ignored -> new java.util.HashSet<>()).add(right);
                    neighbors.computeIfAbsent(right, ignored -> new java.util.HashSet<>()).add(left);
                }
            }

            List<String> ordered = new java.util.ArrayList<>(baseOrder.size());
            java.util.Set<String> chosen = new java.util.HashSet<>();
            String start = baseOrder.get(0);
            for (String variable : baseOrder) {
                int current = degreeByVar.getOrDefault(start, 0);
                int candidate = degreeByVar.getOrDefault(variable, 0);
                if (candidate > current
                        || (candidate == current
                                && position.getOrDefault(variable, Integer.MAX_VALUE) < position.getOrDefault(start,
                                        Integer.MAX_VALUE))) {
                    start = variable;
                }
            }
            ordered.add(start);
            chosen.add(start);

            while (ordered.size() < baseOrder.size()) {
                String best = null;
                int bestShared = -1;
                int bestDegree = -1;
                int bestPos = Integer.MAX_VALUE;
                for (String variable : baseOrder) {
                    if (chosen.contains(variable)) {
                        continue;
                    }
                    int shared = 0;
                    for (String neighbor : neighbors.getOrDefault(variable, Set.of())) {
                        if (chosen.contains(neighbor)) {
                            shared++;
                        }
                    }
                    int degree = degreeByVar.getOrDefault(variable, 0);
                    int pos = position.getOrDefault(variable, Integer.MAX_VALUE);
                    if (best == null
                            || shared > bestShared
                            || (shared == bestShared && degree > bestDegree)
                            || (shared == bestShared && degree == bestDegree && pos < bestPos)) {
                        best = variable;
                        bestShared = shared;
                        bestDegree = degree;
                        bestPos = pos;
                    }
                }
                ordered.add(best);
                chosen.add(best);
            }
            return ordered;
        }
    }

    private double conservativeEstimate(double estimatedCount, double standardError) {
        double clampedCount = Math.max(0.0, estimatedCount);
        double clampedError = Math.max(0.0, standardError);
        double score = clampedCount + config.estimationZ() * clampedError;
        return Double.isFinite(score) ? score : Double.POSITIVE_INFINITY;
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
        for (int i = 0; i < components.size(); i++) {
            long count = resultCounts.get(i);
            Component component = components.get(i);
            String left = component.sourceVarName();
            String right = component.targetVarName();
            minCountByVar.merge(left, count, Math::min);
            minCountByVar.merge(right, count, Math::min);
        }
        return minCountByVar.keySet().stream()
                .sorted(Comparator
                        .comparingLong((String v) -> minCountByVar.getOrDefault(v, Long.MAX_VALUE))
                        .thenComparing(Comparator.naturalOrder()))
                .toList();
    }
}
