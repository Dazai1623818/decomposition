package evaluator.tools;

import evaluator.bench.BenchTypes;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.decomposition.Decomposer;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.ExecutablePlan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.Planner;
import evaluator.index.NativeCpqIndex;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Builds topology-bench workloads with random label injections, validates them
 * against selected indices, and emits combined a123 workload files.
 */
public final class TopologyDiverseWorkloadBuilder {
    private static final Path DEFAULT_SOURCE_METADATA = Path.of(
            "local/queries/topology-bench/workloads/full_queries/basequeries_plus_queries.metadata.csv");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of(
            "local/queries/topology-bench/workloads/diverse_non_zero");
    private static final Path DEFAULT_ADVOGATO = Path.of("local/indices/fork_advogato.idx");
    private static final Path DEFAULT_ROBOTS = Path.of("local/indices/robots.k2.idx");
    private static final Path DEFAULT_WIKIVOTE = Path.of("local/indices/wikivote.idx");
    private static final int[] DEFAULT_SCALES = new int[] { 10, 20, 50 };
    private static final int DEFAULT_LABEL_COUNT = 4;
    private static final int ARITY_COUNT = 3;
    private static final DecompositionMethod ZERO_SCREEN_METHOD = DecompositionMethod.MAX_COLLAPSE;
    private static final String DATASET_ADVOGATO = "advogato";
    private static final String DATASET_WIKIVOTE = "wikivote";
    private static final String DATASET_ROBOTS = "robots";
    private static final String[] DEFAULT_DATASETS = new String[] { DATASET_ADVOGATO, DATASET_WIKIVOTE };
    private static final String[] KNOWN_DATASETS = new String[] { DATASET_ADVOGATO, DATASET_ROBOTS, DATASET_WIKIVOTE };
    private static final int TIMEOUT_BUDGET_DENOMINATOR = 10;
    private static final int DEFAULT_BATCH_SIZE = 56;
    private static final int DEFAULT_POOL_SIZE_FACTOR = 8;
    private static final int DEFAULT_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_QUERY_CONCURRENCY = 1;
    private static final long DEFAULT_SEED = 0x51A7E1234L;
    private static final long HEAD_SELECTION_SEED_MIX = 0x9E3779B97F4A7C15L;
    private static final boolean VALIDATION_SAFE_DISTINCT_FAST_PATH = true;
    private static final String DECOMPOSE_METHODS_PROPERTY = "cpq.decompose.methods";
    private static final String VALIDATION_BACKEND_IN_PROCESS = "in_process";
    private static final String VALIDATION_BACKEND_WORKER_POOL = "worker_pool";
    private static final String EDGE_DIRECTION_FIXED = "fixed_source_orientation";
    private static final String EDGE_DIRECTION_INDEPENDENT = "independent_binary_flip";
    private static final String WORKER_DONE = "__TOPOLOGY_VALIDATION_DONE__";
    private static final String WORKER_STOP = "STOP";
    private static final int WORKER_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private TopologyDiverseWorkloadBuilder() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        new TopologyDiverseWorkloadBuilder().run(options);
    }

    static String randomizedHeadVarsKeyForTesting(String query, int arity, long seed) {
        return QuerySkeleton.parse(query).randomHeadSelection(seed).key(arity);
    }

    static String selectedDatasetsForTesting(String... args) {
        return String.join(",", Options.parse(args).datasets());
    }

    static String ignoredDatasetsForTesting(String... args) {
        Options options = Options.parse(args);
        return String.join(",", ignoredDatasets(options.datasets(), options.allDatasets()));
    }

    static String datasetIndexPathForTesting(String dataset, String... args) {
        return Options.parse(args).indexPath(dataset).toString();
    }

    static int workersPerDatasetForTesting(String... args) {
        return Options.parse(args).workersPerDataset();
    }

    static int queryConcurrencyForTesting(String... args) {
        return Options.parse(args).queryConcurrency();
    }

    static String validationModeForTesting(String... args) {
        return Options.parse(args).validationMode().id();
    }

    static boolean independentEdgeDirectionsForTesting(String... args) {
        return Options.parse(args).independentEdgeDirections();
    }

    static int labelCountForTesting(String... args) {
        return Options.parse(args).labelCount();
    }

    static long candidateSpaceForTesting(String query, int labelCount, boolean independentEdgeDirections) {
        return QuerySkeleton.parse(query).candidateSpaceSize(labelCount, independentEdgeDirections);
    }

    static String renderWithDirectionsForTesting(
            String query,
            int[] labels,
            boolean[] reversed,
            String... headVars) {
        return QuerySkeleton.parse(query).render(labels, reversed, headVars);
    }

    private void run(Options options) throws Exception {
        Files.createDirectories(options.outputDir());
        List<TemplateSpec> templates = loadTemplates(options.sourceMetadata(), options.templateStart(), options.templateCount());
        if (templates.isEmpty()) {
            throw new IllegalStateException("No templates loaded from " + options.sourceMetadata());
        }
        System.out.println(String.format(
                Locale.ROOT,
                "loaded templates=%d source=%s output=%s",
                templates.size(),
                options.sourceMetadata().toAbsolutePath(),
                options.outputDir().toAbsolutePath()));

        String previousMethods = System.getProperty(DECOMPOSE_METHODS_PROPERTY);
        System.clearProperty(DECOMPOSE_METHODS_PROPERTY);
        try {
            List<DatasetEvaluator> evaluators = loadEvaluators(options);

            Map<String, TemplateState> states = initializeStates(
                    templates,
                    options.maxScale(),
                    options.scales(),
                    options.datasets(),
                    options.labelCount(),
                    options.independentEdgeDirections(),
                    options.poolSize(),
                    options.seed());
            try {
                populateAcceptedBodies(templates, states, evaluators, options);
                writeOutputs(templates, states, evaluators, options);
            } finally {
                for (DatasetEvaluator evaluator : evaluators) {
                    evaluator.close();
                }
            }
        } finally {
            if (previousMethods == null) {
                System.clearProperty(DECOMPOSE_METHODS_PROPERTY);
            } else {
                System.setProperty(DECOMPOSE_METHODS_PROPERTY, previousMethods);
            }
        }
    }

    private static List<DatasetEvaluator> loadEvaluators(Options options) throws Exception {
        List<DatasetEvaluator> evaluators = new ArrayList<>();
        for (String dataset : options.datasets()) {
            evaluators.add(loadEvaluator(
                    dataset,
                    options.indexPath(dataset),
                    options.timeoutMs(),
                    options.validationMode(),
                    options.workersPerDataset(),
                    options.outputDir()));
        }
        return List.copyOf(evaluators);
    }

    private static DatasetEvaluator loadEvaluator(
            String name,
            Path indexPath,
            int timeoutMs,
            ValidationMode validationMode,
            int workersPerDataset,
            Path outputDir) throws Exception {
        if (workersPerDataset > 0) {
            System.out.println(String.format(
                    Locale.ROOT,
                    "starting validation workers dataset=%s workers=%d path=%s",
                    name,
                    workersPerDataset,
                    indexPath.toAbsolutePath()));
            DatasetEvaluator evaluator = new WorkerPoolDatasetEvaluator(
                    name,
                    indexPath,
                    timeoutMs,
                    validationMode,
                    workersPerDataset,
                    outputDir);
            System.out.println(String.format(
                    Locale.ROOT,
                    "started validation workers dataset=%s workers=%d timeout_ms=%d validation_mode=%s zero_screen_method=%s",
                    name,
                    workersPerDataset,
                    timeoutMs,
                    validationMode.id(),
                    ZERO_SCREEN_METHOD.id()));
            return evaluator;
        }

        System.out.println(String.format(
                Locale.ROOT,
                "loading index dataset=%s path=%s",
                name,
                indexPath.toAbsolutePath()));
        DatasetEvaluator evaluator = new InProcessDatasetEvaluator(name, indexPath, timeoutMs, validationMode);
        System.out.println(String.format(
                Locale.ROOT,
                "loaded index dataset=%s timeout_ms=%d validation_mode=%s zero_screen_method=%s",
                name,
                timeoutMs,
                validationMode.id(),
                ZERO_SCREEN_METHOD.id()));
        return evaluator;
    }

    private static Map<String, TemplateState> initializeStates(
            List<TemplateSpec> templates,
            int maxAccepted,
            int[] scales,
            String[] datasets,
            int labelCount,
            boolean independentEdgeDirections,
            int poolSize,
            long seed) {
        Map<String, TemplateState> states = new LinkedHashMap<>();
        for (TemplateSpec template : templates) {
            states.put(
                    template.template(),
                    TemplateState.create(
                            template,
                            maxAccepted,
                            scales,
                            datasets,
                            labelCount,
                            independentEdgeDirections,
                            poolSize,
                            seed + (31L * template.workloadId())));
        }
        return states;
    }

    private static void populateAcceptedBodies(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Options options) throws Exception {
        List<TemplateState> orderedStates = new ArrayList<>(states.values());
        int nextTemplateIndex = 0;
        int dispatchRounds = 0;
        int inFlight = 0;
        int completedSinceCheckpoint = 0;
        int checkpointInterval = Math.max(1, Math.min(options.batchSize(), options.queryConcurrency()));
        Map<String, Integer> pendingByTemplate = new HashMap<>();
        ExecutorService candidateExecutor = createCandidateExecutor(options.queryConcurrency());
        CompletionService<CandidateTaskResult> completions = new ExecutorCompletionService<>(candidateExecutor);
        try {
            while (true) {
                int availableSlots = options.queryConcurrency() - inFlight;
                if (availableSlots > 0) {
                    BatchSelection selection = nextBatch(
                            orderedStates,
                            Math.min(options.batchSize(), availableSlots),
                            nextTemplateIndex,
                            pendingByTemplate);
                    List<BatchEntry> batch = selection.batch();
                    nextTemplateIndex = selection.nextStartIndex();
                    if (!batch.isEmpty()) {
                        dispatchRounds++;
                        System.out.println(String.format(
                                Locale.ROOT,
                                "dispatch_round=%d candidates=%d in_flight_before=%d",
                                dispatchRounds,
                                batch.size(),
                                inFlight));
                        for (BatchEntry entry : batch) {
                            pendingByTemplate.merge(entry.template().template(), 1, Integer::sum);
                            completions.submit(() -> validateCandidateAcrossDatasets(entry, evaluators));
                            inFlight++;
                        }
                        continue;
                    }
                }

                if (inFlight == 0) {
                    break;
                }

                CandidateTaskResult result = awaitCandidateResult(completions.take());
                inFlight--;
                pendingByTemplate.computeIfPresent(result.template().template(), (ignored, count) -> count <= 1 ? null : count - 1);
                applyCandidateResult(result, states);
                completedSinceCheckpoint++;
                if (completedSinceCheckpoint >= checkpointInterval || inFlight == 0) {
                    printProgress(states, dispatchRounds);
                    writeOutputs(templates, states, evaluators, options);
                    completedSinceCheckpoint = 0;
                }
            }
        } finally {
            candidateExecutor.shutdownNow();
        }

        List<String> shortfall = new ArrayList<>();
        for (TemplateState state : states.values()) {
            if (!state.supportsScale(state.requiredAccepted())) {
                shortfall.add(state.template().template() + "=" + state.accepted().size());
            }
        }
        if (!shortfall.isEmpty()) {
            System.out.println("shortfall templates=" + String.join(", ", shortfall));
        }
    }

    private static ExecutorService createCandidateExecutor(int queryConcurrency) {
        return Executors.newFixedThreadPool(queryConcurrency, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("topology-builder-candidate");
            thread.setDaemon(true);
            return thread;
        });
    }

    private static CandidateTaskResult validateCandidateAcrossDatasets(
            BatchEntry entry,
            List<DatasetEvaluator> evaluators) {
        List<String> queries = renderQueries(entry);
        Map<String, ArityEvaluations> validations = new LinkedHashMap<>();
        for (DatasetEvaluator evaluator : orderedEvaluators(evaluators)) {
            long started = System.nanoTime();
            List<QueryEvaluation> evaluations = evaluator.evaluateQueries(queries);
            long elapsed = System.nanoTime() - started;
            evaluator.recordValidationStats(elapsed, queries.size());
            if (evaluations.size() != ARITY_COUNT) {
                throw new IllegalStateException(String.format(
                        Locale.ROOT,
                        "Dataset %s returned %d evaluations for %d queries",
                        evaluator.name(),
                        evaluations.size(),
                        ARITY_COUNT));
            }
            ArityEvaluations arityEvaluations = new ArityEvaluations(
                    evaluations.get(0),
                    evaluations.get(1),
                    evaluations.get(2));
            validations.put(evaluator.name(), arityEvaluations);
            String rejectionReason = rejectionReason(evaluator.name(), evaluator.validationMode(), arityEvaluations);
            if (rejectionReason != null) {
                return new CandidateTaskResult(
                        entry.template(),
                        entry.body(),
                        new CandidateValidation(Collections.unmodifiableMap(new LinkedHashMap<>(validations)), rejectionReason));
            }
        }
        return new CandidateTaskResult(
                entry.template(),
                entry.body(),
                new CandidateValidation(Collections.unmodifiableMap(new LinkedHashMap<>(validations)), null));
    }

    private static List<DatasetEvaluator> orderedEvaluators(List<DatasetEvaluator> evaluators) {
        List<DatasetEvaluator> ordered = new ArrayList<>(evaluators);
        ordered.sort((left, right) -> {
            if (left.hasObservedOrderStats() != right.hasObservedOrderStats()) {
                return left.hasObservedOrderStats() ? -1 : 1;
            }
            int cost = Long.compare(left.orderKey(), right.orderKey());
            if (cost != 0) {
                return cost;
            }
            return left.name().compareTo(right.name());
        });
        return List.copyOf(ordered);
    }

    private static CandidateTaskResult awaitCandidateResult(Future<CandidateTaskResult> future) throws Exception {
        try {
            return future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        }
    }

    private static List<String> renderQueries(BatchEntry entry) {
        List<String> queries = new ArrayList<>(ARITY_COUNT);
        for (int arity = 1; arity <= ARITY_COUNT; arity++) {
            queries.add(entry.template().skeleton().render(
                    entry.body().labels(),
                    entry.body().reversed(),
                    entry.body().headSelection().at(arity)));
        }
        return List.copyOf(queries);
    }

    private static void printProgress(Map<String, TemplateState> states, int batchNumber) {
        int completed = 0;
        int total = 0;
        long evaluated = 0L;
        for (TemplateState state : states.values()) {
            completed += state.reportedAcceptedCount();
            total += state.requiredAccepted();
            evaluated += state.evaluatedCount();
        }
        System.out.println(String.format(
                Locale.ROOT,
                "progress time=%s batches=%d accepted=%d/%d evaluated=%d",
                Instant.now(),
                batchNumber,
                completed,
                total,
                evaluated));
    }

    private static void appendValidationColumns(
            StringBuilder builder,
            ArityEvaluations arityEvaluations,
            boolean includeTimeoutCount) {
        if (arityEvaluations == null) {
            builder.append(',');
            if (includeTimeoutCount) {
                builder.append(',');
            }
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                builder.append(',').append("")
                        .append(',').append("")
                        .append(',').append("");
            }
            return;
        }
        builder.append(',').append(arityEvaluations.okCount());
        if (includeTimeoutCount) {
            builder.append(',').append(arityEvaluations.timeoutCount());
        }
        for (int arity = 1; arity <= ARITY_COUNT; arity++) {
            QueryEvaluation evaluation = arityEvaluations.at(arity);
            builder.append(',')
                    .append(evaluation.status()).append(',')
                    .append(evaluation.answers()).append(',')
                    .append(String.format(Locale.ROOT, "%.3f", nanosToMillis(evaluation.wallNanos())));
        }
    }

    private static BatchSelection nextBatch(
            List<TemplateState> states,
            int batchSize,
            int nextStartIndex,
            Map<String, Integer> pendingByTemplate) {
        List<BatchEntry> batch = new ArrayList<>(batchSize);
        Map<String, Integer> scheduledByTemplate = new HashMap<>(pendingByTemplate);
        if (states.isEmpty()) {
            return new BatchSelection(batch, nextStartIndex);
        }
        int stateCount = states.size();
        int index = Math.floorMod(nextStartIndex, stateCount);
        int consecutiveMisses = 0;
        while (batch.size() < batchSize && consecutiveMisses < stateCount) {
            TemplateState state = states.get(index);
            index = (index + 1) % stateCount;
            if (state.samplingComplete()) {
                consecutiveMisses++;
                continue;
            }
            int pending = scheduledByTemplate.getOrDefault(state.template().template(), 0);
            if (state.remainingDispatchSlots(pending) <= 0) {
                consecutiveMisses++;
                continue;
            }
            CandidateBody candidate = state.nextCandidate();
            if (candidate == null) {
                consecutiveMisses++;
                continue;
            }
            batch.add(new BatchEntry(state.template(), candidate));
            scheduledByTemplate.merge(state.template().template(), 1, Integer::sum);
            consecutiveMisses = 0;
        }
        return new BatchSelection(batch, index);
    }

    private record BatchSelection(
            List<BatchEntry> batch,
            int nextStartIndex) {
    }

    private record CandidateTaskResult(
            TemplateSpec template,
            CandidateBody body,
            CandidateValidation validation) {
    }

    private static void applyCandidateResult(
            CandidateTaskResult result,
            Map<String, TemplateState> states) {
        TemplateState state = states.get(result.template().template());
        state.recordEvaluated();
        if (state.observe(result.body(), result.validation())) {
            state.recomputeAccepted();
        }
    }

    private static void writeOutputs(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Options options) throws IOException {
        writeReadme(templates, states, evaluators, options);
        writeAcceptanceSummary(templates, states, evaluators, options.outputDir(), options.scales());
        writeCandidatePool(templates, states, evaluators, options.outputDir());
        for (int scale : options.scales()) {
            writeScaleOutputs(templates, states, evaluators, options.outputDir(), scale);
        }
    }

    private static void writeReadme(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Options options) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("source_metadata=").append(options.sourceMetadata().toAbsolutePath()).append('\n');
        builder.append("templates=").append(templates.size()).append('\n');
        builder.append("template_start=").append(options.templateStart()).append('\n');
        builder.append("template_count=").append(options.templateCount()).append('\n');
        builder.append("scales=").append(joinInts(options.scales())).append('\n');
        builder.append("benchmark_repeats=3\n");
        builder.append("label_injection_strategy=random_without_replacement\n");
        builder.append("label_count=").append(options.labelCount()).append('\n');
        builder.append("edge_direction_injection_strategy=")
                .append(options.edgeDirectionStrategy()).append('\n');
        builder.append("acceptance_datasets=").append(String.join(",", options.datasets())).append('\n');
        builder.append("ignored_datasets=").append(String.join(",", ignoredDatasets(options.datasets(), options.allDatasets())))
                .append('\n');
        builder.append("validation_mode=").append(options.validationMode().id()).append('\n');
        builder.append("acceptance_rule=").append(options.validationMode().acceptanceRule()).append('\n');
        builder.append("timeout_budget_per_shape_per_dataset=disabled\n");
        builder.append("scale_output_rule=each_scale_file_contains_only_templates_with_at_least_that_many_accepted_injections\n");
        builder.append("arity_head_selection=randomized_non_x0_prefix_with_x0_anchor\n");
        builder.append("answer_semantics=").append(options.validationMode().answerSemantics()).append('\n');
        builder.append("zero_screen_method=").append(ZERO_SCREEN_METHOD.id()).append('\n');
        builder.append("zero_screen_rule=").append(options.validationMode().zeroScreenRule()).append('\n');
        builder.append("output_checkpoint_rule=all_outputs_rewritten_after_each_candidate_checkpoint_and_at_completion\n");
        builder.append("timeout_ms=").append(options.timeoutMs()).append('\n');
        builder.append("batch_size=").append(options.batchSize()).append('\n');
        builder.append("candidate_pool_size_per_shape=").append(options.poolSize()).append('\n');
        builder.append("validation_backend=").append(options.validationBackend()).append('\n');
        builder.append("dataset_validation_strategy=pipelined_candidate_short_circuit_on_first_failing_dataset\n");
        builder.append("dataset_order_strategy=observed_fastest_first_with_index_size_fallback\n");
        builder.append("workers_per_dataset=").append(options.workersPerDataset()).append('\n');
        builder.append("query_concurrency=").append(options.queryConcurrency()).append('\n');
        builder.append("seed=").append(options.seed()).append('\n');
        for (DatasetEvaluator evaluator : evaluators) {
            builder.append("index[").append(evaluator.name()).append("]=")
                    .append(evaluator.indexPath().toAbsolutePath()).append('\n');
        }
        long acceptedBodies = 0L;
        long evaluatedBodies = 0L;
        long eligibleBodies = 0L;
        long rejectedBodies = 0L;
        for (TemplateState state : states.values()) {
            acceptedBodies += state.reportedAcceptedCount();
            evaluatedBodies += state.evaluatedCount();
            eligibleBodies += state.eligibleCount();
            rejectedBodies += state.rejectedCount();
        }
        builder.append("accepted_bodies=").append(acceptedBodies).append('\n');
        builder.append("evaluated_bodies=").append(evaluatedBodies).append('\n');
        builder.append("eligible_bodies=").append(eligibleBodies).append('\n');
        builder.append("rejected_bodies=").append(rejectedBodies).append('\n');
        for (int scale : options.scales()) {
            builder.append("templates_meeting_i").append(scale).append('=')
                    .append(countTemplatesMeetingScale(states, scale)).append('/').append(templates.size()).append('\n');
        }
        Files.writeString(options.outputDir().resolve("README.txt"), builder.toString(), StandardCharsets.UTF_8);
    }

    private static void writeAcceptanceSummary(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Path outputDir,
            int[] scales) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("workload_id,template,family,edges,variables,seed_injections,candidate_space,candidate_pool_target,")
                .append("evaluated_bodies,eligible_bodies,rejected_bodies,accepted_bodies,timeout_budget");
        for (int scale : scales) {
            builder.append(",supports_i").append(scale);
        }
        for (DatasetEvaluator evaluator : evaluators) {
            builder.append(',')
                    .append(evaluator.name()).append("_ok_queries,")
                    .append(evaluator.name()).append("_timeout_queries");
        }
        builder.append('\n');
        for (int i = 0; i < templates.size(); i++) {
            TemplateSpec template = templates.get(i);
            TemplateState state = states.get(template.template());
            builder.append(template.workloadId()).append(',')
                    .append(csv(template.template())).append(',')
                    .append(csv(template.family())).append(',')
                    .append(template.skeleton().atomCount()).append(',')
                    .append(template.skeleton().variableCount()).append(',')
                    .append(template.seedCodes().size()).append(',')
                    .append(state.candidateSpace()).append(',')
                    .append(state.sampleTarget()).append(',')
                    .append(state.evaluatedCount()).append(',')
                    .append(state.eligibleCount()).append(',')
                    .append(state.rejectedCount()).append(',')
                    .append(state.reportedAcceptedCount()).append(',')
                    .append(state.timeoutBudget());
            for (int scale : scales) {
                builder.append(',').append(state.supportsScale(scale));
            }
            for (DatasetEvaluator evaluator : evaluators) {
                builder.append(',')
                        .append(state.okQueries(evaluator.name())).append(',')
                        .append(state.timeoutQueries(evaluator.name()));
            }
            builder.append('\n');
        }
        Files.writeString(outputDir.resolve("acceptance_summary.csv"), builder.toString(), StandardCharsets.UTF_8);
    }

    private static int countTemplatesMeetingScale(Map<String, TemplateState> states, int scale) {
        int count = 0;
        for (TemplateState state : states.values()) {
            if (state.supportsScale(scale)) {
                count++;
            }
        }
        return count;
    }

    private static void writeCandidatePool(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Path outputDir) throws IOException {
        Path candidatePoolFile = outputDir.resolve("candidate_pool.csv");
        StringBuilder builder = new StringBuilder();
        builder.append(
                "workload_id,template,family,evaluated_rank,labels,direction_pattern,a1_head_vars,a2_head_vars,a3_head_vars,pool_status,detail,accepted_rank");
        for (DatasetEvaluator evaluator : evaluators) {
            builder.append(',')
                    .append(evaluator.name()).append("_ok_arities,")
                    .append(evaluator.name()).append("_timeout_arities");
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                builder.append(',')
                        .append(evaluator.name()).append("_a").append(arity).append("_status,")
                        .append(evaluator.name()).append("_a").append(arity).append("_answers,")
                        .append(evaluator.name()).append("_a").append(arity).append("_wall_ms");
            }
        }
        builder.append('\n');
        for (TemplateSpec template : templates) {
            TemplateState state = states.get(template.template());
            Map<Long, Integer> acceptedRankByCode = state.acceptedRankByCode();
            for (EvaluatedBody body : state.allCandidates()) {
                Integer acceptedRank = acceptedRankByCode.get(body.code());
                String poolStatus = acceptedRank != null ? "accepted" : "rejected";
                String detail = acceptedRank != null ? acceptedDetail(body.validations()) : body.rejectionReason();
                builder.append(template.workloadId()).append(',')
                        .append(csv(template.template())).append(',')
                        .append(csv(template.family())).append(',')
                        .append(body.evaluatedRank()).append(',')
                        .append(csv(body.labelsKey())).append(',')
                        .append(csv(body.directionKey())).append(',')
                        .append(csv(body.headSelection().key(1))).append(',')
                        .append(csv(body.headSelection().key(2))).append(',')
                        .append(csv(body.headSelection().key(3))).append(',')
                        .append(poolStatus).append(',')
                        .append(csv(detail)).append(',');
                if (acceptedRank != null) {
                    builder.append(acceptedRank);
                }
                for (DatasetEvaluator evaluator : evaluators) {
                    ArityEvaluations arityEvaluations = body.validations().get(evaluator.name());
                    appendValidationColumns(builder, arityEvaluations, true);
                }
                builder.append('\n');
            }
        }
        Files.writeString(candidatePoolFile, builder.toString(), StandardCharsets.UTF_8);
    }

    private static void writeScaleOutputs(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            Path outputDir,
            int scale) throws IOException {
        Path queryFile = outputDir.resolve(String.format(Locale.ROOT, "topology-bench.a123_i%d.cq", scale));
        Path queryMapFile = outputDir.resolve(String.format(Locale.ROOT, "topology-bench.a123_i%d.query_map.csv", scale));
        Path bodyValidationFile = outputDir.resolve(String.format(Locale.ROOT, "topology-bench.a123_i%d.body_validation.csv", scale));

        StringBuilder queries = new StringBuilder();
        StringBuilder queryMap = new StringBuilder();
        StringBuilder validation = new StringBuilder();
        queryMap.append(
                "query_id,workload_id,key,family,template,arity,injection_rank,labels,direction_pattern,head_vars,source,connected,cycle_rank,max_degree\n");
        validation.append("workload_id,template,injection_rank,labels,direction_pattern,a1_head_vars,a2_head_vars,a3_head_vars");
        for (DatasetEvaluator evaluator : evaluators) {
            validation.append(',')
                    .append(evaluator.name()).append("_ok_arities");
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                validation.append(',')
                        .append(evaluator.name()).append("_a").append(arity).append("_status,")
                        .append(evaluator.name()).append("_a").append(arity).append("_answers,")
                        .append(evaluator.name()).append("_a").append(arity).append("_wall_ms");
            }
        }
        validation.append('\n');

        int queryId = 1;
        for (int templateIndex = 0; templateIndex < templates.size(); templateIndex++) {
            TemplateSpec template = templates.get(templateIndex);
            int workloadId = template.workloadId();
            TemplateState state = states.get(template.template());
            if (!state.supportsScale(scale)) {
                continue;
            }
            for (int injectionRank = 0; injectionRank < scale; injectionRank++) {
                EvaluatedBody body = state.accepted().get(injectionRank);
                validation.append(workloadId).append(',')
                        .append(csv(template.template())).append(',')
                        .append(injectionRank + 1).append(',')
                        .append(csv(body.labelsKey())).append(',')
                        .append(csv(body.directionKey())).append(',')
                        .append(csv(body.headSelection().key(1))).append(',')
                        .append(csv(body.headSelection().key(2))).append(',')
                        .append(csv(body.headSelection().key(3)));
                for (DatasetEvaluator evaluator : evaluators) {
                    ArityEvaluations arityEvaluations = body.validations().get(evaluator.name());
                    appendValidationColumns(validation, arityEvaluations, false);
                }
                validation.append('\n');

                for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                    String query = template.skeleton().render(body.labels(), body.reversed(), body.headSelection().at(arity));
                    queries.append(query).append('\n');
                    queryMap.append(queryId).append(',')
                            .append(workloadId).append(',')
                            .append(csv(template.keyFor(scale, arity))).append(',')
                            .append(csv(template.family())).append(',')
                            .append(csv(template.template())).append(',')
                            .append(arity).append(',')
                            .append(injectionRank + 1).append(',')
                            .append(csv(body.labelsKey())).append(',')
                            .append(csv(body.directionKey())).append(',')
                            .append(csv(body.headSelection().key(arity))).append(',')
                            .append("topology_diverse_builder,")
                            .append(template.connected()).append(',')
                            .append(template.cycleRank()).append(',')
                            .append(template.maxDegree()).append('\n');
                    queryId++;
                }
            }
        }

        Files.writeString(queryFile, queries.toString(), StandardCharsets.UTF_8);
        Files.writeString(queryMapFile, queryMap.toString(), StandardCharsets.UTF_8);
        Files.writeString(bodyValidationFile, validation.toString(), StandardCharsets.UTF_8);
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0D;
    }

    private static String joinInts(int[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(values[i]);
        }
        return builder.toString();
    }

    private static String joinLabels(int[] labels) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < labels.length; i++) {
            if (i > 0) {
                builder.append('|');
            }
            builder.append(labels[i]);
        }
        return builder.toString();
    }

    private static String[] ignoredDatasets(String[] selectedDatasets, String[] allDatasets) {
        Set<String> selected = new LinkedHashSet<>(Arrays.asList(selectedDatasets));
        List<String> ignored = new ArrayList<>();
        for (String dataset : allDatasets) {
            if (!selected.contains(dataset)) {
                ignored.add(dataset);
            }
        }
        return ignored.toArray(String[]::new);
    }

    private static List<TemplateSpec> loadTemplates(Path metadataPath, int templateStart, int templateCount) throws IOException {
        List<TemplateSpec> allTemplates = new ArrayList<>();
        Map<String, TemplateBuilder> byTemplate = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                return List.of();
            }
            List<String> headerColumns = parseCsvLine(header);
            Map<String, Integer> indexByColumn = indexByColumn(headerColumns);
            for (String line; (line = reader.readLine()) != null;) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> columns = parseCsvLine(line);
                CsvRow row = new CsvRow(indexByColumn, columns);
                String template = row.value("template");
                TemplateBuilder builder = byTemplate.get(template);
                if (builder == null) {
                    builder = new TemplateBuilder(row);
                    byTemplate.put(template, builder);
                }
                builder.addSeed(row);
            }
        }

        for (TemplateBuilder builder : byTemplate.values()) {
            allTemplates.add(builder.build());
        }
        if (templateStart < 0) {
            throw new IllegalArgumentException("templateStart must be >= 0");
        }
        if (templateCount < 0) {
            throw new IllegalArgumentException("templateCount must be >= 0");
        }
        if (templateStart >= allTemplates.size()) {
            return List.of();
        }
        int toIndex = templateCount == 0
                ? allTemplates.size()
                : Math.min(allTemplates.size(), templateStart + templateCount);
        return List.copyOf(allTemplates.subList(templateStart, toIndex));
    }

    private static Map<String, Integer> indexByColumn(List<String> headerColumns) {
        Map<String, Integer> indexByColumn = new HashMap<>();
        for (int i = 0; i < headerColumns.size(); i++) {
            indexByColumn.put(headerColumns.get(i), i);
        }
        return indexByColumn;
    }

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (quoted && (i + 1) < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (c == ',' && !quoted) {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        fields.add(field.toString());
        return fields;
    }

    private static String csv(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private record BatchEntry(TemplateSpec template, CandidateBody body) {
    }

    private record CandidateValidation(
            Map<String, ArityEvaluations> validations,
            String rejectionReason) {
        private boolean accepted() {
            return rejectionReason == null;
        }
    }

    static record QueryEvaluation(
            BenchTypes.EvalFileStatus status,
            long answers,
            long wallNanos) {
        private boolean isSuccessful() {
            return status == BenchTypes.EvalFileStatus.OK && answers > 0L;
        }

        private boolean isTimeout() {
            return status == BenchTypes.EvalFileStatus.TIMEOUT;
        }

        private boolean isAcceptedOutcome() {
            return isSuccessful() || isTimeout();
        }

        String serialize() {
            return status.name() + '\t' + answers + '\t' + wallNanos;
        }

        static QueryEvaluation parse(String line) {
            String[] parts = line.split("\t", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid topology validation result row: " + line);
            }
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.valueOf(parts[0]),
                    Long.parseLong(parts[1]),
                    Long.parseLong(parts[2]));
        }
    }

    private record ArityEvaluations(
            QueryEvaluation arityOne,
            QueryEvaluation arityTwo,
            QueryEvaluation arityThree) {
        private QueryEvaluation at(int arity) {
            return switch (arity) {
                case 1 -> arityOne;
                case 2 -> arityTwo;
                case 3 -> arityThree;
                default -> throw new IllegalArgumentException("Unsupported arity " + arity);
            };
        }

        private int okCount() {
            int count = 0;
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                QueryEvaluation evaluation = at(arity);
                if (evaluation != null && evaluation.isSuccessful()) {
                    count++;
                }
            }
            return count;
        }

        private int timeoutCount() {
            int count = 0;
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                QueryEvaluation evaluation = at(arity);
                if (evaluation != null && evaluation.isTimeout()) {
                    count++;
                }
            }
            return count;
        }

        private boolean isAcceptedBody() {
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                QueryEvaluation evaluation = at(arity);
                if (evaluation == null) {
                    return false;
                }
                if (!evaluation.isAcceptedOutcome()) {
                    return false;
                }
            }
            return true;
        }
    }

    static boolean definitelyZeroByCompilationForTesting(
            String queryText,
            CpqIndex index,
            DecompositionMethod method,
            int timeoutMs) {
        return screenDefinitelyZero(queryText, index, method, timeoutMs) != null;
    }

    static QueryEvaluation firstAnswerQueryForTesting(
            String queryText,
            CpqIndex index,
            DecompositionMethod method,
            int timeoutMs) {
        return evaluateQuery(queryText, index, new Planner(index), method, timeoutMs, ValidationMode.FIRST_ANSWER);
    }

    static QueryEvaluation quickScanQueryForTesting(
            String queryText,
            CpqIndex index,
            DecompositionMethod method,
            int timeoutMs) {
        return evaluateQuery(queryText, index, new Planner(index), method, timeoutMs, ValidationMode.QUICK_SCAN);
    }

    static boolean acceptedBodyForTesting(
            BenchTypes.EvalFileStatus arityOneStatus,
            long arityOneAnswers,
            BenchTypes.EvalFileStatus arityTwoStatus,
            long arityTwoAnswers,
            BenchTypes.EvalFileStatus arityThreeStatus,
            long arityThreeAnswers) {
        return new ArityEvaluations(
                new QueryEvaluation(arityOneStatus, arityOneAnswers, 0L),
                new QueryEvaluation(arityTwoStatus, arityTwoAnswers, 0L),
                new QueryEvaluation(arityThreeStatus, arityThreeAnswers, 0L))
                .isAcceptedBody();
    }

    static String rejectionReasonForTesting(
            String dataset,
            BenchTypes.EvalFileStatus arityOneStatus,
            long arityOneAnswers,
            BenchTypes.EvalFileStatus arityTwoStatus,
            long arityTwoAnswers,
            BenchTypes.EvalFileStatus arityThreeStatus,
            long arityThreeAnswers) {
        return rejectionReason(
                dataset,
                ValidationMode.FIRST_ANSWER,
                new ArityEvaluations(
                        new QueryEvaluation(arityOneStatus, arityOneAnswers, 0L),
                        new QueryEvaluation(arityTwoStatus, arityTwoAnswers, 0L),
                        new QueryEvaluation(arityThreeStatus, arityThreeAnswers, 0L)));
    }

    static String rejectionReasonForTesting(
            String dataset,
            String validationMode,
            BenchTypes.EvalFileStatus arityOneStatus,
            long arityOneAnswers,
            BenchTypes.EvalFileStatus arityTwoStatus,
            long arityTwoAnswers,
            BenchTypes.EvalFileStatus arityThreeStatus,
            long arityThreeAnswers) {
        return rejectionReason(
                dataset,
                ValidationMode.parse(validationMode),
                new ArityEvaluations(
                        new QueryEvaluation(arityOneStatus, arityOneAnswers, 0L),
                        new QueryEvaluation(arityTwoStatus, arityTwoAnswers, 0L),
                        new QueryEvaluation(arityThreeStatus, arityThreeAnswers, 0L)));
    }

    /**
     * Uses decomposition plus component compilation as a one-sided empty check:
     * if any compiled component is empty, the full CQ answer is definitely zero.
     */
    private static QueryEvaluation screenDefinitelyZero(
            String queryText,
            CpqIndex index,
            DecompositionMethod method,
            int timeoutMs) {
        long started = System.nanoTime();
        long deadlineNanos = Deadline.afterMillis(timeoutMs);
        try {
            ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
            Plan plan = zeroScreenPlan(query, index, method, deadlineNanos);
            if (plan == null) {
                return null;
            }
            ExecutablePlan executable = ExecutablePlan.compile(plan, index, deadlineNanos);
            if (!executable.isEmpty()) {
                return null;
            }
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.OK,
                    0L,
                    System.nanoTime() - started);
        } catch (Deadline.Exceeded | CancellationException | Decomposer.DecompositionTimeoutException ex) {
            return null;
        }
    }

    /**
     * Evaluates whether the query has at least one projected answer while
     * retaining the compile-only empty fast path used to reject definite zeros
     * before join execution.
     */
    static QueryEvaluation evaluateQuery(
            String queryText,
            CpqIndex index,
            Planner planner,
            DecompositionMethod method,
            int timeoutMs,
            ValidationMode validationMode) {
        Objects.requireNonNull(planner, "planner");
        Objects.requireNonNull(validationMode, "validationMode");
        return switch (validationMode) {
            case FIRST_ANSWER -> evaluateFirstAnswer(queryText, index, planner, method, timeoutMs);
            case QUICK_SCAN -> validateComponentsNonEmpty(queryText, index, method, timeoutMs);
        };
    }

    /**
     * Evaluates whether the query has at least one projected answer while
     * retaining the compile-only empty fast path used to reject definite zeros
     * before join execution.
     */
    private static QueryEvaluation evaluateFirstAnswer(
            String queryText,
            CpqIndex index,
            Planner planner,
            DecompositionMethod method,
            int timeoutMs) {
        Objects.requireNonNull(planner, "planner");
        long started = System.nanoTime();
        long deadlineNanos = Deadline.afterMillis(timeoutMs);
        try {
            ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
            Plan plan = zeroScreenPlan(query, index, method, deadlineNanos);
            if (plan == null) {
                return new QueryEvaluation(
                        BenchTypes.EvalFileStatus.NO_DECOMPOSITIONS,
                        -1L,
                        System.nanoTime() - started);
            }
            ExecutablePlan executable = ExecutablePlan.compile(plan, index, deadlineNanos);
            if (executable.isEmpty()) {
                return new QueryEvaluation(
                        BenchTypes.EvalFileStatus.OK,
                        0L,
                        System.nanoTime() - started);
            }
            Planner.JoinOrderPlan orderPlan = planner.selectJoinOrder(executable, false, deadlineNanos);
            LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                    orderPlan.order(),
                    LeapfrogJoin.JoinMode.PROJECTED_EXISTS,
                    VALIDATION_SAFE_DISTINCT_FAST_PATH,
                    deadlineNanos);
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.OK,
                    count.count(),
                    System.nanoTime() - started);
        } catch (Deadline.Exceeded | CancellationException | Decomposer.DecompositionTimeoutException ex) {
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.TIMEOUT,
                    -1L,
                    System.nanoTime() - started);
        } catch (Exception ex) {
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.ERROR,
                    -1L,
                    System.nanoTime() - started);
        }
    }

    /**
     * Validates a query by compiling a collapse decomposition and requiring every
     * compiled component relation to be non-empty. The reported answer count is
     * the minimum compiled component cardinality across that plan.
     */
    private static QueryEvaluation validateComponentsNonEmpty(
            String queryText,
            CpqIndex index,
            DecompositionMethod method,
            int timeoutMs) {
        long started = System.nanoTime();
        long deadlineNanos = Deadline.afterMillis(timeoutMs);
        try {
            ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
            Plan plan = zeroScreenPlan(query, index, method, deadlineNanos);
            if (plan == null) {
                return new QueryEvaluation(
                        BenchTypes.EvalFileStatus.NO_DECOMPOSITIONS,
                        -1L,
                        System.nanoTime() - started);
            }
            ExecutablePlan executable = ExecutablePlan.compile(plan, index, deadlineNanos);
            if (executable.isEmpty()) {
                return new QueryEvaluation(
                        BenchTypes.EvalFileStatus.OK,
                        0L,
                        System.nanoTime() - started);
            }
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.OK,
                    minimumComponentCount(executable.componentCounts()),
                    System.nanoTime() - started);
        } catch (Deadline.Exceeded | CancellationException | Decomposer.DecompositionTimeoutException ex) {
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.TIMEOUT,
                    -1L,
                    System.nanoTime() - started);
        } catch (Exception ex) {
            return new QueryEvaluation(
                    BenchTypes.EvalFileStatus.ERROR,
                    -1L,
                    System.nanoTime() - started);
        }
    }

    private static long minimumComponentCount(List<Long> componentCounts) {
        long minimum = Long.MAX_VALUE;
        for (Long count : componentCounts) {
            if (count == null) {
                continue;
            }
            minimum = Math.min(minimum, count.longValue());
        }
        return minimum == Long.MAX_VALUE ? 0L : minimum;
    }

    private static Plan zeroScreenPlan(
            ConjunctiveQuery query,
            CpqIndex index,
            DecompositionMethod method,
            long deadlineNanos) {
        return switch (method) {
            case SINGLE_EDGE -> ensureIndexableSingleEdge(query, index);
            case MAX_COLLAPSE -> firstPlan(Decomposer.cpqkCoverMaxCollapse(
                    index.k(),
                    1,
                    index::cost,
                    index::supports,
                    deadlineNanos).decompose(query.syntax()));
            default -> throw new IllegalArgumentException("Unsupported zero-screen method: " + method.id());
        };
    }

    private static Plan ensureIndexableSingleEdge(ConjunctiveQuery query, CpqIndex index) {
        Plan plan = query.decomposeSingleEdge();
        for (Component component : plan.components()) {
            if (!index.supports(component.cpq())) {
                return null;
            }
        }
        return plan;
    }

    private static Plan firstPlan(Stream<Plan> plans) {
        try (Stream<Plan> stream = plans) {
            return stream.findFirst().orElse(null);
        }
    }

    private interface DatasetEvaluator extends AutoCloseable {
        String name();

        Path indexPath();

        ValidationMode validationMode();

        List<QueryEvaluation> evaluateQueries(List<String> queries);

        void recordValidationStats(long elapsedNanos, int evaluatedQueries);

        boolean hasObservedOrderStats();

        long orderKey();
    }

    private static final class InProcessDatasetEvaluator implements DatasetEvaluator {
        private final String name;
        private final Path indexPath;
        private final NativeCpqIndex index;
        private final Planner planner;
        private final ValidationMode validationMode;
        private final int timeoutMs;
        private final long initialOrderHint;
        private final AtomicLong observedElapsedNanos = new AtomicLong();
        private final AtomicLong observedQueryCount = new AtomicLong();

        private InProcessDatasetEvaluator(String name, Path indexPath, int timeoutMs, ValidationMode validationMode)
                throws Exception {
            this.name = name;
            this.indexPath = indexPath;
            this.index = NativeCpqIndex.load(indexPath);
            this.planner = new Planner(index);
            this.validationMode = Objects.requireNonNull(validationMode, "validationMode");
            this.timeoutMs = timeoutMs;
            this.initialOrderHint = initialOrderHint(indexPath);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Path indexPath() {
            return indexPath;
        }

        @Override
        public ValidationMode validationMode() {
            return validationMode;
        }

        @Override
        public List<QueryEvaluation> evaluateQueries(List<String> queries) {
            QueryEvaluation[] merged = new QueryEvaluation[queries.size()];
            for (int i = 0; i < queries.size(); i++) {
                merged[i] = evaluateQueryAnswers(queries.get(i));
            }

            List<QueryEvaluation> results = new ArrayList<>();
            for (QueryEvaluation evaluation : merged) {
                if (evaluation == null) {
                    throw new IllegalStateException("Missing merged evaluation for " + name);
                }
                results.add(evaluation);
            }
            return List.copyOf(results);
        }

        private QueryEvaluation evaluateQueryAnswers(String queryText) {
            return TopologyDiverseWorkloadBuilder.evaluateQuery(
                    queryText,
                    index,
                    planner,
                    ZERO_SCREEN_METHOD,
                    timeoutMs,
                    validationMode);
        }

        @Override
        public void recordValidationStats(long elapsedNanos, int evaluatedQueries) {
            if (evaluatedQueries <= 0) {
                return;
            }
            observedElapsedNanos.addAndGet(elapsedNanos);
            observedQueryCount.addAndGet(evaluatedQueries);
        }

        @Override
        public boolean hasObservedOrderStats() {
            return observedQueryCount.get() > 0L;
        }

        @Override
        public long orderKey() {
            if (hasObservedOrderStats()) {
                return observedElapsedNanos.get() / observedQueryCount.get();
            }
            return initialOrderHint;
        }

        @Override
        public void close() {
            // NativeCpqIndex does not expose a close hook.
        }
    }

    private static final class WorkerPoolDatasetEvaluator implements DatasetEvaluator {
        private final String name;
        private final Path indexPath;
        private final List<WorkerProcess> workers;
        private final java.util.concurrent.BlockingQueue<WorkerProcess> availableWorkers;
        private final Path runtimeDir;
        private final ValidationMode validationMode;
        private final AtomicLong batchIds = new AtomicLong();
        private final long initialOrderHint;
        private final AtomicLong observedElapsedNanos = new AtomicLong();
        private final AtomicLong observedQueryCount = new AtomicLong();

        private WorkerPoolDatasetEvaluator(
                String name,
                Path indexPath,
                int timeoutMs,
                ValidationMode validationMode,
                int workerCount,
                Path outputDir) throws Exception {
            this.name = name;
            this.indexPath = indexPath;
            this.validationMode = Objects.requireNonNull(validationMode, "validationMode");
            this.initialOrderHint = initialOrderHint(indexPath);
            this.runtimeDir = Files.createDirectories(outputDir
                    .resolve(".validation-workers")
                    .resolve(name)
                    .resolve("session-" + Instant.now().toEpochMilli()));
            this.workers = launchWorkers(workerCount, timeoutMs, validationMode);
            this.availableWorkers = new java.util.concurrent.ArrayBlockingQueue<>(workerCount);
            for (WorkerProcess worker : workers) {
                availableWorkers.add(worker);
            }
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Path indexPath() {
            return indexPath;
        }

        @Override
        public ValidationMode validationMode() {
            return validationMode;
        }

        @Override
        public List<QueryEvaluation> evaluateQueries(List<String> queries) {
            if (queries.isEmpty()) {
                return List.of();
            }
            try {
                WorkerProcess worker = availableWorkers.take();
                try {
                    return worker.runJob(batchIds.incrementAndGet(), queries, runtimeDir);
                } finally {
                    availableWorkers.put(worker);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }

        private List<WorkerProcess> launchWorkers(
                int workerCount,
                int timeoutMs,
                ValidationMode validationMode) throws Exception {
            List<WorkerProcess> startedWorkers = new ArrayList<>(workerCount);
            try {
                for (int workerId = 1; workerId <= workerCount; workerId++) {
                    startedWorkers.add(WorkerProcess.launch(
                            name,
                            indexPath,
                            timeoutMs,
                            validationMode,
                            runtimeDir,
                            workerId));
                }
                return List.copyOf(startedWorkers);
            } catch (Exception ex) {
                for (WorkerProcess worker : startedWorkers) {
                    worker.close();
                }
                throw ex;
            }
        }

        @Override
        public void recordValidationStats(long elapsedNanos, int evaluatedQueries) {
            if (evaluatedQueries <= 0) {
                return;
            }
            observedElapsedNanos.addAndGet(elapsedNanos);
            observedQueryCount.addAndGet(evaluatedQueries);
        }

        @Override
        public boolean hasObservedOrderStats() {
            return observedQueryCount.get() > 0L;
        }

        @Override
        public long orderKey() {
            if (hasObservedOrderStats()) {
                return observedElapsedNanos.get() / observedQueryCount.get();
            }
            return initialOrderHint;
        }

        @Override
        public void close() {
            for (WorkerProcess worker : workers) {
                worker.close();
            }
        }
    }

    private static long initialOrderHint(Path indexPath) {
        try {
            return Files.size(indexPath);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static final class WorkerProcess implements AutoCloseable {
        private final int workerId;
        private final Path stderrPath;
        private final Process process;
        private final BufferedWriter stdin;
        private final BufferedReader stdout;

        private WorkerProcess(
                int workerId,
                Path stderrPath,
                Process process,
                BufferedWriter stdin,
                BufferedReader stdout) {
            this.workerId = workerId;
            this.stderrPath = stderrPath;
            this.process = process;
            this.stdin = stdin;
            this.stdout = stdout;
        }

        /**
         * Starts a warm validation worker that keeps one dataset index loaded
         * and serves candidate validation jobs over stdin/stdout.
         */
        private static WorkerProcess launch(
                String dataset,
                Path indexPath,
                int timeoutMs,
                ValidationMode validationMode,
                Path runtimeDir,
                int workerId) throws IOException {
            String javaBinary = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            String classpath = System.getProperty("java.class.path");
            if (classpath == null || classpath.isBlank()) {
                throw new IllegalStateException("Missing java.class.path for topology validation worker launch");
            }
            List<String> command = new ArrayList<>();
            command.add(javaBinary);
            String javaLibraryPath = System.getProperty("java.library.path");
            if (javaLibraryPath != null && !javaLibraryPath.isBlank()) {
                command.add("-Djava.library.path=" + javaLibraryPath);
            }
            command.add("-cp");
            command.add(classpath);
            command.add(TopologyValidationWorker.class.getName());
            command.add("--index");
            command.add(indexPath.toString());
            command.add("--timeout-ms");
            command.add(Integer.toString(timeoutMs));
            command.add("--validation-mode");
            command.add(validationMode.id());
            Path stderrPath = runtimeDir.resolve(String.format(Locale.ROOT, "worker%02d.stderr.log", workerId));
            Process process = new ProcessBuilder(command)
                    .directory(Path.of("").toAbsolutePath().toFile())
                    .redirectError(stderrPath.toFile())
                    .start();
            System.out.println(String.format(
                    Locale.ROOT,
                    "started validation worker dataset=%s worker_id=%d pid=%d stderr=%s",
                    dataset,
                    workerId,
                    process.pid(),
                    stderrPath.toAbsolutePath()));
            return new WorkerProcess(
                    workerId,
                    stderrPath,
                    process,
                    new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)),
                    new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)));
        }

        private List<QueryEvaluation> runJob(long batchId, List<String> queries, Path runtimeDir) {
            Path partDir = runtimeDir.resolve(String.format(Locale.ROOT, "job%06d_worker%02d", batchId, workerId));
            Path queriesFile = partDir.resolve("queries.cq");
            Path resultsFile = partDir.resolve("results.tsv");
            Path jobFile = partDir.resolve("job.properties");
            try {
                Files.createDirectories(partDir);
                Files.write(queriesFile, queries, StandardCharsets.UTF_8);
                Properties properties = new Properties();
                properties.setProperty("queries_file", queriesFile.toString());
                properties.setProperty("result_file", resultsFile.toString());
                try (BufferedWriter writer = Files.newBufferedWriter(jobFile, StandardCharsets.UTF_8)) {
                    properties.store(writer, null);
                }

                String response = executeJob(jobFile);
                if (!WORKER_DONE.equals(response)) {
                    throw new IllegalStateException(
                            "Validation worker returned unexpected response: " + response + " stderr=" + stderrPath);
                }

                List<QueryEvaluation> evaluations = readResults(resultsFile);
                if (evaluations.size() != queries.size()) {
                    throw new IllegalStateException(String.format(
                            Locale.ROOT,
                            "Validation worker returned %d rows for %d queries stderr=%s",
                            evaluations.size(),
                            queries.size(),
                            stderrPath));
                }
                cleanupSuccessfulJob(partDir, queriesFile, resultsFile, jobFile);
                return evaluations;
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        private synchronized String executeJob(Path jobFile) throws IOException {
            ensureAlive();
            stdin.write(jobFile.toString());
            stdin.newLine();
            stdin.flush();
            for (String response; (response = stdout.readLine()) != null;) {
                String line = response.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (WORKER_DONE.equals(line)) {
                    return line;
                }
                System.out.println(String.format(
                        Locale.ROOT,
                        "validation worker worker_id=%d stdout=%s",
                        workerId,
                        line));
            }
            throw new IOException("Validation worker exited unexpectedly stderr=" + stderrPath);
        }

        private void ensureAlive() {
            if (!process.isAlive()) {
                throw new IllegalStateException("Validation worker is not running stderr=" + stderrPath);
            }
        }

        private static List<QueryEvaluation> readResults(Path resultsFile) throws IOException {
            List<QueryEvaluation> evaluations = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(resultsFile, StandardCharsets.UTF_8)) {
                for (String line; (line = reader.readLine()) != null;) {
                    String row = line.trim();
                    if (row.isEmpty()) {
                        continue;
                    }
                    evaluations.add(QueryEvaluation.parse(row));
                }
            }
            return List.copyOf(evaluations);
        }

        private static void cleanupSuccessfulJob(
                Path partDir,
                Path queriesFile,
                Path resultsFile,
                Path jobFile) throws IOException {
            Files.deleteIfExists(queriesFile);
            Files.deleteIfExists(resultsFile);
            Files.deleteIfExists(jobFile);
            Files.deleteIfExists(partDir);
        }

        @Override
        public void close() {
            try {
                if (process.isAlive()) {
                    stdin.write(WORKER_STOP);
                    stdin.newLine();
                    stdin.flush();
                }
            } catch (IOException ex) {
                // Fall through to process termination below.
            }
            try {
                stdin.close();
            } catch (IOException ex) {
                // Nothing to do.
            }
            try {
                stdout.close();
            } catch (IOException ex) {
                // Nothing to do.
            }
            try {
                if (!process.waitFor(WORKER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(WORKER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }

    private static final class TemplateBuilder {
        private final int workloadId;
        private final String template;
        private final String family;
        private final String source;
        private final String key;
        private final boolean connected;
        private final int cycleRank;
        private final int maxDegree;
        private final QuerySkeleton skeleton;
        private final Set<String> seedCodes = new LinkedHashSet<>();

        private TemplateBuilder(CsvRow row) {
            this.workloadId = Integer.parseInt(row.value("workload_id"));
            this.template = row.value("template");
            this.family = row.value("family");
            this.source = row.value("source");
            this.key = row.value("key");
            this.connected = Boolean.parseBoolean(row.value("connected"));
            this.cycleRank = Integer.parseInt(row.value("cycle_rank"));
            this.maxDegree = Integer.parseInt(row.value("max_degree"));
            this.skeleton = QuerySkeleton.parse(row.value("query"));
        }

        private void addSeed(CsvRow row) {
            if (!"1".equals(row.value("copy_id"))) {
                return;
            }
            seedCodes.add(joinLabels(skeleton.labelsFromQuery(row.value("query"))));
        }

        private TemplateSpec build() {
            return new TemplateSpec(
                    workloadId,
                    template,
                    family,
                    source,
                    key,
                    connected,
                    cycleRank,
                    maxDegree,
                    skeleton,
                    List.copyOf(seedCodes));
        }
    }

    private record TemplateSpec(
            int workloadId,
            String template,
            String family,
            String source,
            String key,
            boolean connected,
            int cycleRank,
            int maxDegree,
            QuerySkeleton skeleton,
            List<String> seedCodes) {
        private String keyFor(int scale, int arity) {
            return template + "_a" + arity + "_i" + scale;
        }
    }

    private static final class TemplateState {
        private final TemplateSpec template;
        private final int requiredAccepted;
        private final int[] scaleCheckpoints;
        private final String[] datasets;
        private final int labelCount;
        private final boolean independentEdgeDirections;
        private final long candidateSpace;
        private final long permutationOffset;
        private final long permutationStep;
        private final long sampleTarget;
        private final long selectionSeed;
        private final List<EvaluatedBody> eligible = new ArrayList<>();
        private final List<EvaluatedBody> allCandidates = new ArrayList<>();
        private final List<EvaluatedBody> accepted = new ArrayList<>();
        private final Map<String, Integer> timeoutCounts = new LinkedHashMap<>();
        private final int timeoutRadix;
        private long nextCandidateIndex;
        private long evaluatedCount;

        private TemplateState(
                TemplateSpec template,
                int requiredAccepted,
                int[] scaleCheckpoints,
                String[] datasets,
                int labelCount,
                boolean independentEdgeDirections,
                long candidateSpace,
                long permutationOffset,
                long permutationStep,
                long sampleTarget,
                long selectionSeed) {
            this.template = template;
            this.requiredAccepted = requiredAccepted;
            this.scaleCheckpoints = Arrays.copyOf(scaleCheckpoints, scaleCheckpoints.length);
            this.datasets = Arrays.copyOf(datasets, datasets.length);
            this.labelCount = labelCount;
            this.independentEdgeDirections = independentEdgeDirections;
            this.candidateSpace = candidateSpace;
            this.permutationOffset = permutationOffset;
            this.permutationStep = permutationStep;
            this.sampleTarget = sampleTarget;
            this.selectionSeed = selectionSeed;
            this.timeoutRadix = timeoutBudget(requiredAccepted) + 1;
        }

        private static TemplateState create(
                TemplateSpec template,
                int requiredAccepted,
                int[] scaleCheckpoints,
                String[] datasets,
                int labelCount,
                boolean independentEdgeDirections,
                int poolSize,
                long seed) {
            long totalCodes = template.skeleton().candidateSpaceSize(labelCount, independentEdgeDirections);
            long sampleTarget = poolSize == 0 ? totalCodes : Math.min(totalCodes, (long) poolSize);
            return new TemplateState(
                    template,
                    requiredAccepted,
                    scaleCheckpoints,
                    datasets,
                    labelCount,
                    independentEdgeDirections,
                    totalCodes,
                    randomOffset(totalCodes, seed),
                    randomCoprimeStep(totalCodes, seed),
                    sampleTarget,
                    seed);
        }

        /**
         * Produces a deterministic full-cycle permutation over the candidate
         * space without materialising all candidate codes in memory.
         */
        private static long randomOffset(long candidateSpace, long seed) {
            if (candidateSpace <= 1) {
                return 0L;
            }
            return new SplittableRandom(seed).nextLong(candidateSpace);
        }

        private static long randomCoprimeStep(long candidateSpace, long seed) {
            if (candidateSpace <= 1) {
                return 1L;
            }
            SplittableRandom random = new SplittableRandom(seed ^ HEAD_SELECTION_SEED_MIX);
            long step;
            do {
                step = random.nextLong(candidateSpace);
            } while (step == 0L || gcd(step, candidateSpace) != 1L);
            return step;
        }

        private static long gcd(long left, long right) {
            long a = Math.abs(left);
            long b = Math.abs(right);
            while (b != 0L) {
                long next = a % b;
                a = b;
                b = next;
            }
            return a;
        }

        private TemplateSpec template() {
            return template;
        }

        private List<EvaluatedBody> accepted() {
            return accepted;
        }

        private List<EvaluatedBody> allCandidates() {
            return allCandidates;
        }

        private int requiredAccepted() {
            return requiredAccepted;
        }

        private int reportedAcceptedCount() {
            return Math.min(accepted.size(), requiredAccepted);
        }

        private long sampleTarget() {
            return sampleTarget;
        }

        private long candidateSpace() {
            return candidateSpace;
        }

        private int eligibleCount() {
            return eligible.size();
        }

        private int rejectedCount() {
            return allCandidates.size() - eligible.size();
        }

        private long evaluatedCount() {
            return evaluatedCount;
        }

        private void recordEvaluated() {
            evaluatedCount++;
        }

        private boolean samplingComplete() {
            return accepted.size() >= requiredAccepted
                    || evaluatedCount >= sampleTarget
                    || nextCandidateIndex >= candidateSpace;
        }

        private boolean supportsScale(int scale) {
            return accepted.size() >= scale;
        }

        /**
         * Bounds in-flight oversubscription so a topology can keep the machine
         * busy after the indices are loaded without exceeding its sample budget.
         */
        private long remainingDispatchSlots(int pendingCount) {
            return Math.max(0L, sampleTarget - evaluatedCount - pendingCount);
        }

        private CandidateBody nextCandidate() {
            if (samplingComplete()) {
                return null;
            }
            while (nextCandidateIndex < candidateSpace) {
                long code = permutedCode(nextCandidateIndex++);
                CandidateEncoding candidate = template.skeleton().decodeCandidate(
                        code,
                        labelCount,
                        independentEdgeDirections);
                return new CandidateBody(
                        code,
                        candidate.labels(),
                        candidate.reversed(),
                        template.skeleton().randomHeadSelection(headSelectionSeed(code)));
            }
            return null;
        }

        private long permutedCode(long ordinal) {
            long offset = multiplyMod(ordinal, permutationStep, candidateSpace);
            return addMod(permutationOffset, offset, candidateSpace);
        }

        private static long multiplyMod(long left, long right, long modulus) {
            long result = 0L;
            long addend = left % modulus;
            long multiplier = right;
            while (multiplier > 0L) {
                if ((multiplier & 1L) != 0L) {
                    result = addMod(result, addend, modulus);
                }
                multiplier >>>= 1;
                if (multiplier != 0L) {
                    addend = addMod(addend, addend, modulus);
                }
            }
            return result;
        }

        private static long addMod(long left, long right, long modulus) {
            long normalizedLeft = left % modulus;
            long normalizedRight = right % modulus;
            if (normalizedLeft >= modulus - normalizedRight) {
                return normalizedLeft - (modulus - normalizedRight);
            }
            return normalizedLeft + normalizedRight;
        }

        private long headSelectionSeed(long code) {
            return selectionSeed + (HEAD_SELECTION_SEED_MIX * (code + 1L));
        }

        private boolean observe(CandidateBody body, CandidateValidation validation) {
            if (!validation.accepted()) {
                allCandidates.add(new EvaluatedBody(
                        evaluatedCount,
                        body.code(),
                        body.labels(),
                        body.reversed(),
                        body.headSelection(),
                        validation.validations(),
                        validation.rejectionReason()));
                return false;
            }
            EvaluatedBody evaluatedBody = new EvaluatedBody(
                    evaluatedCount,
                    body.code(),
                    body.labels(),
                    body.reversed(),
                    body.headSelection(),
                    validation.validations(),
                    null);
            eligible.add(evaluatedBody);
            allCandidates.add(evaluatedBody);
            return true;
        }

        private void recomputeAccepted() {
            accepted.clear();
            timeoutCounts.clear();
            if (eligible.isEmpty()) {
                return;
            }
            accepted.addAll(eligible);
            rebuildTimeoutCounts();
        }

        private void rebuildTimeoutCounts() {
            for (int i = accepted.size() - 1; i >= 0; i--) {
                EvaluatedBody body = accepted.get(i);
                for (String dataset : datasets) {
                    ArityEvaluations validations = body.validations().get(dataset);
                    if (validations != null) {
                        timeoutCounts.merge(dataset, validations.timeoutCount(), Integer::sum);
                    }
                }
            }
        }

        private int okQueries(String dataset) {
            int total = 0;
            for (EvaluatedBody body : accepted) {
                ArityEvaluations validations = body.validations().get(dataset);
                if (validations != null) {
                    total += validations.okCount();
                }
            }
            return total;
        }

        private int timeoutQueries(String dataset) {
            return timeoutCounts.getOrDefault(dataset, 0);
        }

        private int timeoutBudget() {
            return 0;
        }

        private int timeoutBudget(int acceptedCount) {
            return 0;
        }

        private int timeoutStateSpace() {
            int space = 1;
            for (int i = 0; i < datasets.length; i++) {
                space *= timeoutRadix;
            }
            return space;
        }

        private int[] timeoutContribution(EvaluatedBody body) {
            int[] contribution = new int[datasets.length];
            for (int i = 0; i < datasets.length; i++) {
                ArityEvaluations validations = body.validations().get(datasets[i]);
                contribution[i] = validations == null ? 0 : validations.timeoutCount();
            }
            return contribution;
        }

        private int addTimeoutContribution(int stateCode, int[] contribution) {
            int result = 0;
            int factor = 1;
            int remaining = stateCode;
            int maxBudget = timeoutBudget();
            for (int i = 0; i < datasets.length; i++) {
                int current = remaining % timeoutRadix;
                remaining /= timeoutRadix;
                int next = current + contribution[i];
                if (next > maxBudget) {
                    return -1;
                }
                result += next * factor;
                factor *= timeoutRadix;
            }
            return result;
        }

        private boolean withinCheckpointBudget(int selectedCount, int stateCode) {
            if (!isCheckpoint(selectedCount)) {
                return true;
            }
            int budget = timeoutBudget(selectedCount);
            int remaining = stateCode;
            for (int i = 0; i < datasets.length; i++) {
                int count = remaining % timeoutRadix;
                remaining /= timeoutRadix;
                if (count > budget) {
                    return false;
                }
            }
            return true;
        }

        private boolean isCheckpoint(int selectedCount) {
            for (int checkpoint : scaleCheckpoints) {
                if (checkpoint == selectedCount) {
                    return true;
                }
            }
            return false;
        }

        private int bestSelectedCount(boolean[][] reachable) {
            for (int selected = requiredAccepted; selected > 0; selected--) {
                for (boolean state : reachable[selected]) {
                    if (state) {
                        return selected;
                    }
                }
            }
            return 0;
        }

        private int randomReachableStateCode(boolean[] reachableStates, long seed) {
            int reachableCount = 0;
            for (int stateCode = 0; stateCode < reachableStates.length; stateCode++) {
                if (!reachableStates[stateCode]) {
                    continue;
                }
                reachableCount++;
            }
            if (reachableCount == 0) {
                return -1;
            }
            int choice = new SplittableRandom(seed).nextInt(reachableCount);
            for (int stateCode = 0; stateCode < reachableStates.length; stateCode++) {
                if (!reachableStates[stateCode]) {
                    continue;
                }
                if (choice == 0) {
                    return stateCode;
                }
                choice--;
            }
            return -1;
        }

        private Map<Long, Integer> acceptedRankByCode() {
            Map<Long, Integer> byCode = new HashMap<>();
            for (int i = 0; i < accepted.size(); i++) {
                byCode.put(accepted.get(i).code(), i + 1);
            }
            return byCode;
        }
    }

    private static String rejectionReason(
            String dataset,
            ValidationMode validationMode,
            ArityEvaluations validations) {
        for (int arity = 1; arity <= ARITY_COUNT; arity++) {
            QueryEvaluation evaluation = validations.at(arity);
            if (evaluation == null) {
                return dataset + "_a" + arity + "_missing_evaluation";
            }
            if (evaluation.status() == BenchTypes.EvalFileStatus.OK && evaluation.answers() <= 0L) {
                return dataset + "_a" + arity + '_' + validationMode.emptyRejectionSuffix();
            }
            if (evaluation.status() == BenchTypes.EvalFileStatus.TIMEOUT) {
                continue;
            }
            if (evaluation.status() != BenchTypes.EvalFileStatus.OK) {
                return dataset + "_a" + arity + "_status_"
                        + evaluation.status().name().toLowerCase(Locale.ROOT);
            }
        }
        return null;
    }

    private static String acceptedDetail(Map<String, ArityEvaluations> validations) {
        for (ArityEvaluations arities : validations.values()) {
            if (arities.timeoutCount() > 0) {
                return "accepted_timeout_allowed_body";
            }
        }
        return "accepted_positive_validation";
    }

    enum ValidationMode {
        FIRST_ANSWER(
                "first_answer",
                "single_phase_accept_positive_or_timeout;candidate_pool_is_random_sample_without_replacement;accepted_candidates_must_have_at_least_one_projected_answer_or_timeout_on_selected_datasets;non_timeout_results_require_projected_answer_exists",
                "projected_answer_exists",
                "max_collapse_compile_empty_implies_zero;otherwise_acceptance_uses_projected_answer_exists",
                "zero_answers"),
        QUICK_SCAN(
                "quick_scan",
                "single_phase_accept_positive_or_timeout;candidate_pool_is_random_sample_without_replacement;accepted_candidates_must_have_positive_compiled_component_counts_or_timeout_on_selected_datasets;non_timeout_results_require_every_compiled_component_non_empty",
                "min_compiled_component_cardinality",
                "max_collapse_compile_empty_implies_zero;otherwise_acceptance_uses_compiled_component_cardinality",
                "empty_component");

        private final String id;
        private final String acceptanceRule;
        private final String answerSemantics;
        private final String zeroScreenRule;
        private final String emptyRejectionSuffix;

        ValidationMode(
                String id,
                String acceptanceRule,
                String answerSemantics,
                String zeroScreenRule,
                String emptyRejectionSuffix) {
            this.id = id;
            this.acceptanceRule = acceptanceRule;
            this.answerSemantics = answerSemantics;
            this.zeroScreenRule = zeroScreenRule;
            this.emptyRejectionSuffix = emptyRejectionSuffix;
        }

        String id() {
            return id;
        }

        String acceptanceRule() {
            return acceptanceRule;
        }

        String answerSemantics() {
            return answerSemantics;
        }

        String zeroScreenRule() {
            return zeroScreenRule;
        }

        String emptyRejectionSuffix() {
            return emptyRejectionSuffix;
        }

        static ValidationMode parse(String raw) {
            if (raw == null || raw.isBlank()) {
                throw new IllegalArgumentException("validation mode must not be blank");
            }
            String normalized = raw.trim().toLowerCase(Locale.ROOT);
            return switch (normalized) {
                case "first", "first_answer", "first_match", "at_least_one", "at_least_one_answer", "exists" ->
                    FIRST_ANSWER;
                case "quick", "quick_scan", "component_counts", "component_count", "compiled_component_counts" ->
                    QUICK_SCAN;
                default -> throw new IllegalArgumentException("Unknown validation mode: " + raw);
            };
        }
    }

    private record CandidateBody(
            long code,
            int[] labels,
            boolean[] reversed,
            HeadSelection headSelection) {
    }

    private record SelectionStep(
            int previousStateCode,
            int candidateOrderIndex) {
    }

    private record EvaluatedBody(
            long evaluatedRank,
            long code,
            int[] labels,
            boolean[] reversed,
            HeadSelection headSelection,
            Map<String, ArityEvaluations> validations,
            String rejectionReason) {
        private String labelsKey() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < labels.length; i++) {
                if (i > 0) {
                    builder.append('|');
                }
                builder.append(labels[i]);
            }
            return builder.toString();
        }

        private String directionKey() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < reversed.length; i++) {
                if (i > 0) {
                    builder.append('|');
                }
                builder.append(reversed[i] ? 'R' : 'F');
            }
            return builder.toString();
        }
    }

    private record HeadSelection(
            String[] arityOne,
            String[] arityTwo,
            String[] arityThree) {
        private String[] at(int arity) {
            return switch (arity) {
                case 1 -> arityOne;
                case 2 -> arityTwo;
                case 3 -> arityThree;
                default -> throw new IllegalArgumentException("Unsupported arity " + arity);
            };
        }

        private String key(int arity) {
            return String.join("|", at(arity));
        }
    }

    private static final class QuerySkeleton {
        private final List<AtomShape> atoms;
        private final List<String> variableOrder;

        private QuerySkeleton(List<AtomShape> atoms, List<String> variableOrder) {
            this.atoms = List.copyOf(atoms);
            this.variableOrder = List.copyOf(variableOrder);
        }

        private static QuerySkeleton parse(String query) {
            int arrow = query.indexOf('←');
            if (arrow < 0) {
                throw new IllegalArgumentException("Invalid query: missing arrow in " + query);
            }
            String body = query.substring(arrow + 1).trim();
            List<AtomShape> atoms = new ArrayList<>();
            List<String> variableOrder = new ArrayList<>();
            int index = 0;
            while (index < body.length()) {
                while (index < body.length() && (body.charAt(index) == ' ' || body.charAt(index) == ',')) {
                    index++;
                }
                if (index >= body.length()) {
                    break;
                }
                int labelEnd = body.indexOf('(', index);
                int comma = body.indexOf(',', labelEnd + 1);
                int close = body.indexOf(')', comma + 1);
                int label = Integer.parseInt(body.substring(index, labelEnd).trim());
                String source = body.substring(labelEnd + 1, comma).trim();
                String target = body.substring(comma + 1, close).trim();
                atoms.add(new AtomShape(source, target));
                if (!variableOrder.contains(source)) {
                    variableOrder.add(source);
                }
                if (!variableOrder.contains(target)) {
                    variableOrder.add(target);
                }
                index = close + 1;
                if (label < 0) {
                    throw new IllegalArgumentException("Unexpected label in source skeleton: " + query);
                }
            }
            if (atoms.isEmpty()) {
                throw new IllegalArgumentException("Invalid query: no atoms in " + query);
            }
            return new QuerySkeleton(atoms, variableOrder);
        }

        private int atomCount() {
            return atoms.size();
        }

        private int variableCount() {
            return variableOrder.size();
        }

        private long candidateSpaceSize(int labelCount, boolean useIndependentEdgeDirections) {
            long total = 1L;
            long edgeRadix = edgeRadix(labelCount, useIndependentEdgeDirections);
            for (int i = 0; i < atoms.size(); i++) {
                total = Math.multiplyExact(total, edgeRadix);
            }
            return total;
        }

        private int[] labelsFromQuery(String query) {
            int arrow = query.indexOf('←');
            String body = query.substring(arrow + 1).trim();
            int[] labels = new int[atoms.size()];
            int index = 0;
            int atom = 0;
            while (index < body.length() && atom < labels.length) {
                while (index < body.length() && (body.charAt(index) == ' ' || body.charAt(index) == ',')) {
                    index++;
                }
                if (index >= body.length()) {
                    break;
                }
                int labelEnd = body.indexOf('(', index);
                labels[atom++] = Integer.parseInt(body.substring(index, labelEnd).trim());
                int close = body.indexOf(')', labelEnd + 1);
                index = close + 1;
            }
            return labels;
        }

        private CandidateEncoding decodeCandidate(long code, int labelCount, boolean independentEdgeDirections) {
            int[] labels = new int[atoms.size()];
            boolean[] reversed = new boolean[atoms.size()];
            long remaining = code;
            long edgeRadix = edgeRadix(labelCount, independentEdgeDirections);
            for (int i = 0; i < atoms.size(); i++) {
                long edgeCode = remaining % edgeRadix;
                labels[i] = (int) (edgeCode % labelCount);
                reversed[i] = independentEdgeDirections && edgeCode >= labelCount;
                remaining /= edgeRadix;
            }
            return new CandidateEncoding(labels, reversed);
        }

        private static long edgeRadix(int labelCount, boolean independentEdgeDirections) {
            return independentEdgeDirections ? Math.multiplyExact((long) labelCount, 2L) : labelCount;
        }

        private String render(int[] labels, boolean[] reversed, String[] headVars) {
            StringBuilder builder = new StringBuilder();
            builder.append('(');
            for (int i = 0; i < headVars.length; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(headVars[i]);
            }
            builder.append(") ← ");
            for (int i = 0; i < atoms.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                AtomShape atom = atoms.get(i);
                String source = reversed[i] ? atom.target() : atom.source();
                String target = reversed[i] ? atom.source() : atom.target();
                builder.append(labels[i]).append('(')
                        .append(source).append(',')
                        .append(target).append(')');
            }
            return builder.toString();
        }

        private HeadSelection randomHeadSelection(long seed) {
            List<String> shuffled = new ArrayList<>(variableOrder.size());
            for (String variable : variableOrder) {
                if (!"x0".equals(variable)) {
                    shuffled.add(variable);
                }
            }
            shuffleStrings(shuffled, seed);
            return new HeadSelection(
                    headVarsPrefix(shuffled, 1),
                    headVarsPrefix(shuffled, 2),
                    headVarsPrefix(shuffled, 3));
        }

        private String[] headVarsPrefix(List<String> shuffledVariables, int arity) {
            if (arity < 1 || arity > ARITY_COUNT) {
                throw new IllegalArgumentException("Unsupported arity " + arity);
            }
            String[] head = new String[arity];
            head[0] = "x0";
            if ((arity - 1) > shuffledVariables.size()) {
                throw new IllegalArgumentException("Template lacks enough variables for arity " + arity);
            }
            for (int i = 1; i < arity; i++) {
                head[i] = shuffledVariables.get(i - 1);
            }
            return head;
        }

        private static void shuffleStrings(List<String> values, long seed) {
            SplittableRandom random = new SplittableRandom(seed);
            for (int i = values.size() - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                String tmp = values.get(i);
                values.set(i, values.get(j));
                values.set(j, tmp);
            }
        }

    }

    private record CandidateEncoding(int[] labels, boolean[] reversed) {
    }

    private record AtomShape(String source, String target) {
    }

    private static final class CsvRow {
        private final Map<String, Integer> indexByColumn;
        private final List<String> values;

        private CsvRow(Map<String, Integer> indexByColumn, List<String> values) {
            this.indexByColumn = indexByColumn;
            this.values = values;
        }

        private String value(String column) {
            Integer index = indexByColumn.get(column);
            if (index == null || index >= values.size()) {
                throw new IllegalArgumentException("Missing column " + column);
            }
            return values.get(index);
        }
    }

    private record Options(
            Path sourceMetadata,
            Path outputDir,
            Map<String, Path> datasetIndexes,
            String[] datasets,
            String[] allDatasets,
            int[] scales,
            int batchSize,
            int poolSize,
            ValidationMode validationMode,
            int labelCount,
            int workersPerDataset,
            boolean independentEdgeDirections,
            int queryConcurrency,
            int timeoutMs,
            long seed,
            int templateStart,
            int templateCount) {
        private static Options parse(String[] args) {
            Path sourceMetadata = DEFAULT_SOURCE_METADATA;
            Path outputDir = DEFAULT_OUTPUT_DIR;
            Map<String, Path> defaultDatasetIndexes = defaultDatasetIndexes();
            LinkedHashMap<String, Path> customDatasetIndexes = new LinkedHashMap<>();
            String rawDatasets = null;
            int[] scales = Arrays.copyOf(DEFAULT_SCALES, DEFAULT_SCALES.length);
            int batchSize = DEFAULT_BATCH_SIZE;
            int poolSize = -1;
            ValidationMode validationMode = ValidationMode.FIRST_ANSWER;
            int labelCount = DEFAULT_LABEL_COUNT;
            int workersPerDataset = 0;
            boolean independentEdgeDirections = false;
            int queryConcurrency = DEFAULT_QUERY_CONCURRENCY;
            int timeoutMs = DEFAULT_TIMEOUT_MS;
            long seed = DEFAULT_SEED;
            int templateStart = 0;
            int templateCount = 0;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--source-metadata" -> sourceMetadata = Path.of(requireValue(args, ++i, "--source-metadata"));
                    case "--output-dir" -> outputDir = Path.of(requireValue(args, ++i, "--output-dir"));
                    case "--advogato-index" -> defaultDatasetIndexes.put(DATASET_ADVOGATO,
                            Path.of(requireValue(args, ++i, "--advogato-index")));
                    case "--robots-index" -> defaultDatasetIndexes.put(DATASET_ROBOTS,
                            Path.of(requireValue(args, ++i, "--robots-index")));
                    case "--wikivote-index" -> defaultDatasetIndexes.put(DATASET_WIKIVOTE,
                            Path.of(requireValue(args, ++i, "--wikivote-index")));
                    case "--dataset-index" -> {
                        DatasetSpec spec = parseDatasetSpec(requireValue(args, ++i, "--dataset-index"));
                        customDatasetIndexes.put(spec.name(), spec.indexPath());
                    }
                    case "--datasets" -> rawDatasets = requireValue(args, ++i, "--datasets");
                    case "--scales" -> scales = parseScales(requireValue(args, ++i, "--scales"));
                    case "--batch-size" -> batchSize = Integer.parseInt(requireValue(args, ++i, "--batch-size"));
                    case "--pool-size" -> poolSize = Integer.parseInt(requireValue(args, ++i, "--pool-size"));
                    case "--validation-mode" -> validationMode = ValidationMode.parse(
                            requireValue(args, ++i, "--validation-mode"));
                    case "--label-count" -> labelCount = Integer.parseInt(requireValue(args, ++i, "--label-count"));
                    case "--workers-per-dataset" -> workersPerDataset = Integer.parseInt(
                            requireValue(args, ++i, "--workers-per-dataset"));
                    case "--independent-edge-directions" -> independentEdgeDirections = true;
                    case "--query-concurrency" -> queryConcurrency = Integer.parseInt(
                            requireValue(args, ++i, "--query-concurrency"));
                    case "--timeout-ms" -> timeoutMs = Integer.parseInt(requireValue(args, ++i, "--timeout-ms"));
                    case "--seed" -> seed = Long.parseLong(requireValue(args, ++i, "--seed"));
                    case "--template-start" -> templateStart = Integer.parseInt(requireValue(args, ++i, "--template-start"));
                    case "--template-count", "--template-limit" -> templateCount = Integer.parseInt(
                            requireValue(args, ++i, arg));
                    default -> throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            if (poolSize < 0) {
                poolSize = maxScale(scales) * DEFAULT_POOL_SIZE_FACTOR;
            }
            if (labelCount <= 0) {
                throw new IllegalArgumentException("--label-count must be > 0");
            }
            if (workersPerDataset < 0) {
                throw new IllegalArgumentException("--workers-per-dataset must be >= 0");
            }
            if (queryConcurrency <= 0) {
                throw new IllegalArgumentException("--query-concurrency must be > 0");
            }
            LinkedHashMap<String, Path> datasetIndexes = new LinkedHashMap<>(defaultDatasetIndexes);
            datasetIndexes.putAll(customDatasetIndexes);
            String[] allDatasets = rawDatasets == null && !customDatasetIndexes.isEmpty()
                    ? datasetNames(customDatasetIndexes)
                    : datasetNames(datasetIndexes);
            String[] datasets = rawDatasets != null
                    ? parseDatasets(rawDatasets, allDatasets)
                    : defaultSelectedDatasets(customDatasetIndexes);
            return new Options(
                    sourceMetadata,
                    outputDir,
                    Collections.unmodifiableMap(datasetIndexes),
                    datasets,
                    allDatasets,
                    scales,
                    batchSize,
                    poolSize,
                    validationMode,
                    labelCount,
                    workersPerDataset,
                    independentEdgeDirections,
                    queryConcurrency,
                    timeoutMs,
                    seed,
                    templateStart,
                    templateCount);
        }

        private int maxScale() {
            return maxScale(scales);
        }

        private String validationBackend() {
            return workersPerDataset > 0 ? VALIDATION_BACKEND_WORKER_POOL : VALIDATION_BACKEND_IN_PROCESS;
        }

        private String edgeDirectionStrategy() {
            return independentEdgeDirections ? EDGE_DIRECTION_INDEPENDENT : EDGE_DIRECTION_FIXED;
        }

        private Path indexPath(String dataset) {
            Path indexPath = datasetIndexes.get(dataset);
            if (indexPath == null) {
                throw new IllegalArgumentException("Unsupported dataset: " + dataset);
            }
            return indexPath;
        }

        private static int maxScale(int[] scales) {
            int max = 0;
            for (int scale : scales) {
                max = Math.max(max, scale);
            }
            return max;
        }

        private static Map<String, Path> defaultDatasetIndexes() {
            LinkedHashMap<String, Path> datasetIndexes = new LinkedHashMap<>();
            datasetIndexes.put(DATASET_ADVOGATO, DEFAULT_ADVOGATO);
            datasetIndexes.put(DATASET_ROBOTS, DEFAULT_ROBOTS);
            datasetIndexes.put(DATASET_WIKIVOTE, DEFAULT_WIKIVOTE);
            return datasetIndexes;
        }

        private static String[] datasetNames(Map<String, Path> datasetIndexes) {
            return datasetIndexes.keySet().toArray(String[]::new);
        }

        private static String[] defaultSelectedDatasets(Map<String, Path> customDatasetIndexes) {
            if (customDatasetIndexes.isEmpty()) {
                return Arrays.copyOf(DEFAULT_DATASETS, DEFAULT_DATASETS.length);
            }
            return customDatasetIndexes.keySet().toArray(String[]::new);
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " requires a value");
            }
            return args[index];
        }

        private static DatasetSpec parseDatasetSpec(String raw) {
            int separator = raw.indexOf('=');
            if (separator <= 0 || separator == (raw.length() - 1)) {
                throw new IllegalArgumentException("--dataset-index requires name=path");
            }
            String name = raw.substring(0, separator).trim();
            String path = raw.substring(separator + 1).trim();
            if (name.isEmpty() || path.isEmpty()) {
                throw new IllegalArgumentException("--dataset-index requires name=path");
            }
            return new DatasetSpec(name, Path.of(path));
        }

        private static int[] parseScales(String raw) {
            String[] parts = raw.split(",");
            int[] scales = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                scales[i] = Integer.parseInt(parts[i].trim());
            }
            Arrays.sort(scales);
            return scales;
        }

        private static String[] parseDatasets(String raw, String[] availableDatasets) {
            String[] parts = raw.split(",");
            LinkedHashSet<String> datasets = new LinkedHashSet<>();
            Set<String> available = new LinkedHashSet<>(Arrays.asList(availableDatasets));
            for (String part : parts) {
                String dataset = part.trim();
                if (dataset.isEmpty()) {
                    continue;
                }
                if (!available.contains(dataset)) {
                    throw new IllegalArgumentException("Unknown dataset: " + dataset);
                }
                datasets.add(dataset);
            }
            if (datasets.isEmpty()) {
                throw new IllegalArgumentException("--datasets requires at least one known dataset");
            }
            return datasets.toArray(String[]::new);
        }
    }

    private record DatasetSpec(String name, Path indexPath) {
    }
}
