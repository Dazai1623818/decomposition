package evaluator.tools;

import evaluator.bench.BenchRunner;
import evaluator.bench.BenchTypes;
import evaluator.bench.EngineConfig;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.decomposition.Decomposer;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.ExecutablePlan;
import evaluator.index.NativeCpqIndex;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    private static final int[] LABELS = new int[] { 0, 1, 2, 3 };
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
    private static final long DEFAULT_SEED = 0x51A7E1234L;
    private static final long HEAD_SELECTION_SEED_MIX = 0x9E3779B97F4A7C15L;
    private static final String DECOMPOSE_METHODS_PROPERTY = "cpq.decompose.methods";

    private TopologyDiverseWorkloadBuilder() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        new TopologyDiverseWorkloadBuilder().run(options);
    }

    static String randomizedHeadVarsKeyForTesting(String query, int arity, long seed) {
        return QuerySkeleton.parse(query).randomHeadSelection(seed).key(arity);
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

        System.setProperty(DECOMPOSE_METHODS_PROPERTY, "single_edge");
        List<DatasetEvaluator> evaluators = loadEvaluators(options);
        ExecutorService datasetExecutor = createDatasetExecutor(evaluators.size());

        Map<String, TemplateState> states = initializeStates(templates, options.maxScale(), options.scales(), options.datasets(),
                options.poolSize(), options.seed());
        try {
            populateAcceptedBodies(templates, states, evaluators, datasetExecutor, options);
            writeOutputs(templates, states, evaluators, options);
        } finally {
            datasetExecutor.shutdownNow();
            for (DatasetEvaluator evaluator : evaluators) {
                evaluator.close();
            }
        }
    }

    private static List<DatasetEvaluator> loadEvaluators(Options options) throws Exception {
        List<DatasetEvaluator> evaluators = new ArrayList<>();
        for (String dataset : options.datasets()) {
            Path indexPath = switch (dataset) {
                case DATASET_ADVOGATO -> options.advogatoIndex();
                case DATASET_ROBOTS -> options.robotsIndex();
                case DATASET_WIKIVOTE -> options.wikivoteIndex();
                default -> throw new IllegalArgumentException("Unsupported dataset: " + dataset);
            };
            evaluators.add(loadEvaluator(dataset, indexPath, options.timeoutMs(), options.seed()));
        }
        return List.copyOf(evaluators);
    }

    private static ExecutorService createDatasetExecutor(int datasetCount) {
        int threads = Math.max(1, datasetCount);
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("topology-builder-dataset");
            thread.setDaemon(true);
            return thread;
        });
    }

    private static DatasetEvaluator loadEvaluator(String name, Path indexPath, int timeoutMs, long seed) throws Exception {
        System.out.println(String.format(
                Locale.ROOT,
                "loading index dataset=%s path=%s",
                name,
                indexPath.toAbsolutePath()));
        DatasetEvaluator evaluator = new DatasetEvaluator(name, indexPath, timeoutMs, seed);
        System.out.println(String.format(
                Locale.ROOT,
                "loaded index dataset=%s timeout_ms=%d zero_screen_method=%s",
                name,
                timeoutMs,
                ZERO_SCREEN_METHOD.id()));
        return evaluator;
    }

    private static Map<String, TemplateState> initializeStates(
            List<TemplateSpec> templates,
            int maxAccepted,
            int[] scales,
            String[] datasets,
            int poolSize,
            long seed) {
        Map<String, TemplateState> states = new LinkedHashMap<>();
        for (TemplateSpec template : templates) {
            states.put(template.template(), TemplateState.create(template, maxAccepted, scales, datasets, poolSize,
                    seed + (31L * template.workloadId())));
        }
        return states;
    }

    private static void populateAcceptedBodies(
            List<TemplateSpec> templates,
            Map<String, TemplateState> states,
            List<DatasetEvaluator> evaluators,
            ExecutorService datasetExecutor,
            Options options) throws Exception {
        int batchNumber = 0;
        List<TemplateState> orderedStates = new ArrayList<>(states.values());
        int nextTemplateIndex = 0;
        while (true) {
            BatchSelection selection = nextBatch(orderedStates, options.batchSize(), nextTemplateIndex);
            List<BatchEntry> batch = selection.batch();
            nextTemplateIndex = selection.nextStartIndex();
            if (batch.isEmpty()) {
                break;
            }
            batchNumber++;
            Path queriesFile = writeBatchQueries(batch);
            try {
                Map<String, List<QueryEvaluation>> byDataset = evaluateBatchAcrossDatasets(
                        batchNumber,
                        batch,
                        queriesFile,
                        evaluators,
                        datasetExecutor);
                applyBatchResults(batch, byDataset, states);
            } finally {
                Files.deleteIfExists(queriesFile);
            }

            printProgress(states, batchNumber);
            writeOutputs(templates, states, evaluators, options);
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

    private static Map<String, List<QueryEvaluation>> evaluateBatchAcrossDatasets(
            int batchNumber,
            List<BatchEntry> batch,
            Path queriesFile,
            List<DatasetEvaluator> evaluators,
            ExecutorService datasetExecutor) throws Exception {
        Map<String, Future<DatasetBatchResult>> futures = new LinkedHashMap<>();
        for (DatasetEvaluator evaluator : evaluators) {
            System.out.println(String.format(
                    Locale.ROOT,
                    "batch=%d candidates=%d dataset=%s evaluating_queries=%d",
                    batchNumber,
                    batch.size(),
                    evaluator.name(),
                    batch.size() * ARITY_COUNT));
            futures.put(evaluator.name(), datasetExecutor.submit(() -> {
                long start = System.nanoTime();
                List<QueryEvaluation> evaluations = evaluator.evaluateBatch(queriesFile);
                long elapsed = System.nanoTime() - start;
                return new DatasetBatchResult(
                        evaluator.name(),
                        evaluations,
                        elapsed,
                        evaluator.screenedZeroQueries(),
                        evaluator.fullEvaluationQueries());
            }));
        }
        Map<String, List<QueryEvaluation>> byDataset = new LinkedHashMap<>();
        for (DatasetEvaluator evaluator : evaluators) {
            DatasetBatchResult result = awaitDatasetBatchResult(futures.get(evaluator.name()));
            byDataset.put(result.dataset(), result.evaluations());
            System.out.println(String.format(
                    Locale.ROOT,
                    "batch=%d dataset=%s done ok_queries=%d screened_zero_queries=%d full_eval_queries=%d elapsed_ms=%.3f",
                    batchNumber,
                    result.dataset(),
                    successfulCount(result.evaluations()),
                    result.screenedZeroQueries(),
                    result.evaluatedQueries(),
                    nanosToMillis(result.elapsedNanos())));
        }
        return byDataset;
    }

    private static DatasetBatchResult awaitDatasetBatchResult(Future<DatasetBatchResult> future) throws Exception {
        try {
            return future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        }
    }

    private static void printProgress(Map<String, TemplateState> states, int batchNumber) {
        int completed = 0;
        int total = 0;
        int evaluated = 0;
        for (TemplateState state : states.values()) {
            completed += state.accepted().size();
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

    private static int successfulCount(List<QueryEvaluation> evaluations) {
        int count = 0;
        for (QueryEvaluation evaluation : evaluations) {
            if (evaluation.isSuccessful()) {
                count++;
            }
        }
        return count;
    }

    private static BatchSelection nextBatch(List<TemplateState> states, int batchSize, int nextStartIndex) {
        List<BatchEntry> batch = new ArrayList<>(batchSize);
        Map<String, Integer> pendingByTemplate = new HashMap<>();
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
            int pending = pendingByTemplate.getOrDefault(state.template().template(), 0);
            if (pending >= state.remainingAcceptedSlots()) {
                consecutiveMisses++;
                continue;
            }
            CandidateBody candidate = state.nextCandidate();
            if (candidate == null) {
                consecutiveMisses++;
                continue;
            }
            batch.add(new BatchEntry(state.template(), candidate));
            pendingByTemplate.merge(state.template().template(), 1, Integer::sum);
            consecutiveMisses = 0;
        }
        return new BatchSelection(batch, index);
    }

    private record BatchSelection(
            List<BatchEntry> batch,
            int nextStartIndex) {
    }

    private static void applyBatchResults(
            List<BatchEntry> batch,
            Map<String, List<QueryEvaluation>> byDataset,
            Map<String, TemplateState> states) {
        Set<String> dirtyTemplates = new LinkedHashSet<>();
        for (int i = 0; i < batch.size(); i++) {
            BatchEntry entry = batch.get(i);
            TemplateState state = states.get(entry.template().template());
            Map<String, ArityEvaluations> evaluations = new LinkedHashMap<>();
            for (Map.Entry<String, List<QueryEvaluation>> datasetEntry : byDataset.entrySet()) {
                int offset = i * ARITY_COUNT;
                ArityEvaluations arityEvaluations = new ArityEvaluations(
                        datasetEntry.getValue().get(offset),
                        datasetEntry.getValue().get(offset + 1),
                        datasetEntry.getValue().get(offset + 2));
                evaluations.put(datasetEntry.getKey(), arityEvaluations);
            }

            state.recordEvaluated();
            if (state.observe(entry.body(), evaluations)) {
                dirtyTemplates.add(entry.template().template());
            }
        }
        for (String template : dirtyTemplates) {
            states.get(template).recomputeAccepted();
        }
    }

    private static Path writeBatchQueries(List<BatchEntry> batch) throws IOException {
        List<String> lines = new ArrayList<>(batch.size() * ARITY_COUNT);
        for (BatchEntry entry : batch) {
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                lines.add(entry.template().skeleton().render(entry.body().labels(), entry.body().headSelection().at(arity)));
            }
        }
        return writeQueryFile(lines);
    }

    private static Path writeQueryFile(List<String> queries) throws IOException {
        Path path = Files.createTempFile("topology-diverse-batch", ".cq");
        Files.write(path, queries, StandardCharsets.UTF_8);
        return path;
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
        builder.append("acceptance_datasets=").append(String.join(",", options.datasets())).append('\n');
        builder.append("ignored_datasets=").append(String.join(",", ignoredDatasets(options.datasets()))).append('\n');
        builder.append(
                "acceptance_rule=single_phase_accept_positive_or_timeout;candidate_pool_is_random_sample_without_replacement;accepted_candidates_must_be_positive_or_timeout_on_selected_datasets;non_timeout_results_must_be_positive\n");
        builder.append("timeout_budget_per_shape_per_dataset=disabled\n");
        builder.append("scale_output_rule=each_scale_file_contains_only_templates_with_at_least_that_many_accepted_injections\n");
        builder.append("arity_head_selection=randomized_non_x0_prefix_with_x0_anchor\n");
        builder.append("zero_screen_method=").append(ZERO_SCREEN_METHOD.id()).append('\n');
        builder.append("zero_screen_rule=compile_only_empty_plan_implies_zero_else_full_count_eval\n");
        builder.append("output_checkpoint_rule=all_outputs_rewritten_after_each_completed_batch_and_at_completion\n");
        builder.append("timeout_ms=").append(options.timeoutMs()).append('\n');
        builder.append("batch_size=").append(options.batchSize()).append('\n');
        builder.append("candidate_pool_size_per_shape=").append(options.poolSize()).append('\n');
        builder.append("seed=").append(options.seed()).append('\n');
        for (DatasetEvaluator evaluator : evaluators) {
            builder.append("index[").append(evaluator.name()).append("]=")
                    .append(evaluator.indexPath().toAbsolutePath()).append('\n');
        }
        int acceptedBodies = 0;
        int evaluatedBodies = 0;
        int eligibleBodies = 0;
        int rejectedBodies = 0;
        for (TemplateState state : states.values()) {
            acceptedBodies += state.accepted().size();
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
                    .append(state.accepted().size()).append(',')
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
        builder.append("workload_id,template,family,evaluated_rank,labels,a1_head_vars,a2_head_vars,a3_head_vars,pool_status,detail,accepted_rank");
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
            Map<Integer, Integer> acceptedRankByCode = state.acceptedRankByCode();
            for (EvaluatedBody body : state.allCandidates()) {
                Integer acceptedRank = acceptedRankByCode.get(body.code());
                String poolStatus = acceptedRank != null ? "accepted" : "rejected";
                String detail = acceptedRank != null ? acceptedDetail(body.validations()) : body.rejectionReason();
                builder.append(template.workloadId()).append(',')
                        .append(csv(template.template())).append(',')
                        .append(csv(template.family())).append(',')
                        .append(body.evaluatedRank()).append(',')
                        .append(csv(body.labelsKey())).append(',')
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
                    builder.append(',')
                            .append(arityEvaluations.okCount()).append(',')
                            .append(arityEvaluations.timeoutCount());
                    for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                        QueryEvaluation evaluation = arityEvaluations.at(arity);
                        builder.append(',')
                                .append(evaluation.status()).append(',')
                                .append(evaluation.answers()).append(',')
                                .append(String.format(Locale.ROOT, "%.3f", nanosToMillis(evaluation.wallNanos())));
                    }
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
        queryMap.append("query_id,workload_id,key,family,template,arity,injection_rank,labels,head_vars,source,connected,cycle_rank,max_degree\n");
        validation.append("workload_id,template,injection_rank,labels,a1_head_vars,a2_head_vars,a3_head_vars");
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
                        .append(csv(body.headSelection().key(1))).append(',')
                        .append(csv(body.headSelection().key(2))).append(',')
                        .append(csv(body.headSelection().key(3)));
                for (DatasetEvaluator evaluator : evaluators) {
                    ArityEvaluations arityEvaluations = body.validations().get(evaluator.name());
                    validation.append(',')
                            .append(arityEvaluations.okCount());
                    for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                        QueryEvaluation evaluation = arityEvaluations.at(arity);
                        validation.append(',')
                                .append(evaluation.status()).append(',')
                                .append(evaluation.answers()).append(',')
                                .append(String.format(Locale.ROOT, "%.3f", nanosToMillis(evaluation.wallNanos())));
                    }
                }
                validation.append('\n');

                for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                    String query = template.skeleton().render(body.labels(), body.headSelection().at(arity));
                    queries.append(query).append('\n');
                    queryMap.append(queryId).append(',')
                            .append(workloadId).append(',')
                            .append(csv(template.keyFor(scale, arity))).append(',')
                            .append(csv(template.family())).append(',')
                            .append(csv(template.template())).append(',')
                            .append(arity).append(',')
                            .append(injectionRank + 1).append(',')
                            .append(csv(body.labelsKey())).append(',')
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

    private static String[] ignoredDatasets(String[] selectedDatasets) {
        Set<String> selected = new LinkedHashSet<>(Arrays.asList(selectedDatasets));
        List<String> ignored = new ArrayList<>();
        for (String dataset : KNOWN_DATASETS) {
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

    private record DatasetBatchResult(
            String dataset,
            List<QueryEvaluation> evaluations,
            long elapsedNanos,
            int screenedZeroQueries,
            int evaluatedQueries) {
    }

    private record QueryEvaluation(
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

    private static final class DatasetEvaluator implements AutoCloseable {
        private final String name;
        private final Path indexPath;
        private final NativeCpqIndex index;
        private final BenchRunner runner;
        private final int timeoutMs;
        private final long seed;

        private DatasetEvaluator(String name, Path indexPath, int timeoutMs, long seed) throws Exception {
            this.name = name;
            this.indexPath = indexPath;
            this.index = NativeCpqIndex.load(indexPath);
            this.runner = new BenchRunner(index, EngineConfig.defaults().withEstimationSeed(seed));
            this.timeoutMs = timeoutMs;
            this.seed = seed;
        }

        private String name() {
            return name;
        }

        private Path indexPath() {
            return indexPath;
        }

        private List<QueryEvaluation> evaluateBatch(Path queriesFile) {
            List<String> queries = loadQueries(queriesFile);
            QueryEvaluation[] merged = new QueryEvaluation[queries.size()];
            List<String> unresolvedQueries = new ArrayList<>();
            List<Integer> unresolvedIndexes = new ArrayList<>();
            int screenedZeroQueries = 0;
            for (int i = 0; i < queries.size(); i++) {
                QueryEvaluation screened = screenDefinitelyZero(queries.get(i));
                if (screened != null) {
                    merged[i] = screened;
                    screenedZeroQueries++;
                    continue;
                }
                unresolvedIndexes.add(i);
                unresolvedQueries.add(queries.get(i));
            }

            if (!unresolvedQueries.isEmpty()) {
                Path unresolvedFile;
                try {
                    unresolvedFile = writeQueryFile(unresolvedQueries);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                try {
                    List<QueryEvaluation> evaluated = evaluateQueries(unresolvedFile, unresolvedQueries.size());
                    for (int i = 0; i < evaluated.size(); i++) {
                        merged[unresolvedIndexes.get(i)] = evaluated.get(i);
                    }
                } finally {
                    try {
                        Files.deleteIfExists(unresolvedFile);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            }

            List<QueryEvaluation> results = new ArrayList<>();
            for (QueryEvaluation evaluation : merged) {
                if (evaluation == null) {
                    throw new IllegalStateException("Missing merged evaluation for " + name);
                }
                results.add(evaluation);
            }
            lastBatchScreenedZeroQueries = screenedZeroQueries;
            lastBatchEvaluatedQueries = unresolvedQueries.size();
            return List.copyOf(results);
        }

        private List<QueryEvaluation> evaluateQueries(Path queriesFile, int expected) {
            List<QueryEvaluation> results = new ArrayList<>();
            BenchTypes.EvalFileProgressSink sink = progress -> results.add(new QueryEvaluation(
                    progress.status(),
                    progress.answers(),
                    progress.wallNanos()));
            BenchTypes.EvalFileSpec spec = new BenchTypes.EvalFileSpec(
                    indexPath,
                    queriesFile,
                    BenchTypes.EvaluationMode.COUNT,
                    ZERO_SCREEN_METHOD,
                    1,
                    0,
                    timeoutMs,
                    sink);
            runner.evalFile(spec);
            if (results.size() != expected) {
                throw new IllegalStateException(
                        "Expected " + expected + " results from " + name + " but got " + results.size());
            }
            return List.copyOf(results);
        }

        private QueryEvaluation screenDefinitelyZero(String queryText) {
            return TopologyDiverseWorkloadBuilder.screenDefinitelyZero(queryText, index, ZERO_SCREEN_METHOD, timeoutMs);
        }

        private int screenedZeroQueries() {
            return lastBatchScreenedZeroQueries;
        }

        private int fullEvaluationQueries() {
            return lastBatchEvaluatedQueries;
        }

        private static List<String> loadQueries(Path queriesFile) {
            try {
                List<String> queries = new ArrayList<>();
                try (BufferedReader reader = Files.newBufferedReader(queriesFile, StandardCharsets.UTF_8)) {
                    for (String line; (line = reader.readLine()) != null;) {
                        String query = line.trim();
                        if (query.isEmpty() || query.startsWith("#")) {
                            continue;
                        }
                        queries.add(query);
                    }
                }
                return List.copyOf(queries);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void close() {
            // NativeCpqIndex does not expose a close hook.
        }

        private int lastBatchScreenedZeroQueries;
        private int lastBatchEvaluatedQueries;
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
        private final Set<Integer> seedCodes = new LinkedHashSet<>();

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
            seedCodes.add(skeleton.encodeLabels(skeleton.labelsFromQuery(row.value("query"))));
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
            List<Integer> seedCodes) {
        private String keyFor(int scale, int arity) {
            return template + "_a" + arity + "_i" + scale;
        }
    }

    private static final class TemplateState {
        private final TemplateSpec template;
        private final int requiredAccepted;
        private final int[] scaleCheckpoints;
        private final String[] datasets;
        private final int[] candidateOrder;
        private final int sampleTarget;
        private final long selectionSeed;
        private final List<EvaluatedBody> eligible = new ArrayList<>();
        private final List<EvaluatedBody> allCandidates = new ArrayList<>();
        private final List<EvaluatedBody> accepted = new ArrayList<>();
        private final Map<String, Integer> timeoutCounts = new LinkedHashMap<>();
        private final int timeoutRadix;
        private int nextCandidateIndex;
        private int evaluatedCount;

        private TemplateState(
                TemplateSpec template,
                int requiredAccepted,
                int[] scaleCheckpoints,
                String[] datasets,
                int[] candidateOrder,
                int sampleTarget,
                long selectionSeed) {
            this.template = template;
            this.requiredAccepted = requiredAccepted;
            this.scaleCheckpoints = Arrays.copyOf(scaleCheckpoints, scaleCheckpoints.length);
            this.datasets = Arrays.copyOf(datasets, datasets.length);
            this.candidateOrder = candidateOrder;
            this.sampleTarget = sampleTarget;
            this.selectionSeed = selectionSeed;
            this.timeoutRadix = timeoutBudget(requiredAccepted) + 1;
        }

        private static TemplateState create(
                TemplateSpec template,
                int requiredAccepted,
                int[] scaleCheckpoints,
                String[] datasets,
                int poolSize,
                long seed) {
            int totalCodes = template.skeleton().labelSpaceSize();
            int[] order = randomOrder(totalCodes, seed);
            int sampleTarget = poolSize == 0 ? totalCodes : Math.min(totalCodes, poolSize);
            return new TemplateState(template, requiredAccepted, scaleCheckpoints, datasets, order, sampleTarget, seed);
        }

        /**
         * Produces a deterministic random traversal of the label-assignment
         * space so each injection is tested at most once per shape.
         */
        private static int[] randomOrder(int totalCodes, long seed) {
            int[] order = new int[totalCodes];
            for (int i = 0; i < totalCodes; i++) {
                order[i] = i;
            }
            shuffle(order, seed);
            return order;
        }

        private static void shuffle(int[] values, long seed) {
            SplittableRandom random = new SplittableRandom(seed);
            for (int i = values.length - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                int tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }
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

        private int sampleTarget() {
            return sampleTarget;
        }

        private int candidateSpace() {
            return candidateOrder.length;
        }

        private int eligibleCount() {
            return eligible.size();
        }

        private int rejectedCount() {
            return allCandidates.size() - eligible.size();
        }

        private int evaluatedCount() {
            return evaluatedCount;
        }

        private void recordEvaluated() {
            evaluatedCount++;
        }

        private boolean samplingComplete() {
            return accepted.size() >= requiredAccepted
                    || evaluatedCount >= sampleTarget
                    || nextCandidateIndex >= candidateOrder.length;
        }

        private boolean supportsScale(int scale) {
            return accepted.size() >= scale;
        }

        private int remainingAcceptedSlots() {
            return Math.max(0, requiredAccepted - accepted.size());
        }

        private CandidateBody nextCandidate() {
            if (samplingComplete()) {
                return null;
            }
            while (nextCandidateIndex < candidateOrder.length) {
                int code = candidateOrder[nextCandidateIndex++];
                int[] labels = template.skeleton().decodeLabels(code);
                return new CandidateBody(code, labels, template.skeleton().randomHeadSelection(headSelectionSeed(code)));
            }
            return null;
        }

        private long headSelectionSeed(int code) {
            return selectionSeed + (HEAD_SELECTION_SEED_MIX * (code + 1L));
        }

        private boolean observe(CandidateBody body, Map<String, ArityEvaluations> validations) {
            for (ArityEvaluations arities : validations.values()) {
                if (!arities.isAcceptedBody()) {
                    allCandidates.add(new EvaluatedBody(
                            evaluatedCount,
                            body.code(),
                            body.labels(),
                            body.headSelection(),
                            validations,
                            rejectionReason(validations)));
                    return false;
                }
            }
            EvaluatedBody evaluatedBody = new EvaluatedBody(
                    evaluatedCount,
                    body.code(),
                    body.labels(),
                    body.headSelection(),
                    validations,
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

        private Map<Integer, Integer> acceptedRankByCode() {
            Map<Integer, Integer> byCode = new HashMap<>();
            for (int i = 0; i < accepted.size(); i++) {
                byCode.put(accepted.get(i).code(), i + 1);
            }
            return byCode;
        }
    }

    private static String rejectionReason(Map<String, ArityEvaluations> validations) {
        boolean sawTimeout = false;
        for (Map.Entry<String, ArityEvaluations> datasetEntry : validations.entrySet()) {
            for (int arity = 1; arity <= ARITY_COUNT; arity++) {
                QueryEvaluation evaluation = datasetEntry.getValue().at(arity);
                if (evaluation == null) {
                    return "missing_evaluation";
                }
                if (evaluation.status() == BenchTypes.EvalFileStatus.OK && evaluation.answers() <= 0L) {
                    return "zero_answers";
                }
                if (evaluation.status() == BenchTypes.EvalFileStatus.TIMEOUT) {
                    sawTimeout = true;
                    continue;
                }
                if (evaluation.status() != BenchTypes.EvalFileStatus.OK) {
                    return "status_" + evaluation.status().name().toLowerCase(Locale.ROOT);
                }
            }
        }
        return sawTimeout ? "timeout" : "rejected";
    }

    private static String acceptedDetail(Map<String, ArityEvaluations> validations) {
        for (ArityEvaluations arities : validations.values()) {
            if (arities.timeoutCount() > 0) {
                return "accepted_timeout_allowed_body";
            }
        }
        return "accepted_non_zero_body";
    }

    private record CandidateBody(
            int code,
            int[] labels,
            HeadSelection headSelection) {
    }

    private record SelectionStep(
            int previousStateCode,
            int candidateOrderIndex) {
    }

    private record EvaluatedBody(
            int evaluatedRank,
            int code,
            int[] labels,
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
                if (label < 0 || label > 3) {
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

        private int labelSpaceSize() {
            int total = 1;
            for (int i = 0; i < atoms.size(); i++) {
                total *= LABELS.length;
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

        private int encodeLabels(int[] labels) {
            int code = 0;
            int factor = 1;
            for (int label : labels) {
                code += label * factor;
                factor *= LABELS.length;
            }
            return code;
        }

        private int[] decodeLabels(int code) {
            int[] labels = new int[atoms.size()];
            int remaining = code;
            for (int i = 0; i < atoms.size(); i++) {
                labels[i] = remaining % LABELS.length;
                remaining /= LABELS.length;
            }
            return labels;
        }

        private String render(int[] labels, String[] headVars) {
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
                builder.append(labels[i]).append('(')
                        .append(atom.source()).append(',')
                        .append(atom.target()).append(')');
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
            Path advogatoIndex,
            Path robotsIndex,
            Path wikivoteIndex,
            String[] datasets,
            int[] scales,
            int batchSize,
            int poolSize,
            int timeoutMs,
            long seed,
            int templateStart,
            int templateCount) {
        private static Options parse(String[] args) {
            Path sourceMetadata = DEFAULT_SOURCE_METADATA;
            Path outputDir = DEFAULT_OUTPUT_DIR;
            Path advogato = DEFAULT_ADVOGATO;
            Path robots = DEFAULT_ROBOTS;
            Path wikivote = DEFAULT_WIKIVOTE;
            String[] datasets = Arrays.copyOf(DEFAULT_DATASETS, DEFAULT_DATASETS.length);
            int[] scales = Arrays.copyOf(DEFAULT_SCALES, DEFAULT_SCALES.length);
            int batchSize = DEFAULT_BATCH_SIZE;
            int poolSize = -1;
            int timeoutMs = DEFAULT_TIMEOUT_MS;
            long seed = DEFAULT_SEED;
            int templateStart = 0;
            int templateCount = 0;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--source-metadata" -> sourceMetadata = Path.of(requireValue(args, ++i, "--source-metadata"));
                    case "--output-dir" -> outputDir = Path.of(requireValue(args, ++i, "--output-dir"));
                    case "--advogato-index" -> advogato = Path.of(requireValue(args, ++i, "--advogato-index"));
                    case "--robots-index" -> robots = Path.of(requireValue(args, ++i, "--robots-index"));
                    case "--wikivote-index" -> wikivote = Path.of(requireValue(args, ++i, "--wikivote-index"));
                    case "--datasets" -> datasets = parseDatasets(requireValue(args, ++i, "--datasets"));
                    case "--scales" -> scales = parseScales(requireValue(args, ++i, "--scales"));
                    case "--batch-size" -> batchSize = Integer.parseInt(requireValue(args, ++i, "--batch-size"));
                    case "--pool-size" -> poolSize = Integer.parseInt(requireValue(args, ++i, "--pool-size"));
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
            return new Options(sourceMetadata, outputDir, advogato, robots, wikivote, datasets, scales, batchSize,
                    poolSize, timeoutMs, seed, templateStart, templateCount);
        }

        private int maxScale() {
            return maxScale(scales);
        }

        private static int maxScale(int[] scales) {
            int max = 0;
            for (int scale : scales) {
                max = Math.max(max, scale);
            }
            return max;
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " requires a value");
            }
            return args[index];
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

        private static String[] parseDatasets(String raw) {
            String[] parts = raw.split(",");
            LinkedHashSet<String> datasets = new LinkedHashSet<>();
            for (String part : parts) {
                String dataset = part.trim();
                if (dataset.isEmpty()) {
                    continue;
                }
                boolean known = false;
                for (String candidate : KNOWN_DATASETS) {
                    if (candidate.equals(dataset)) {
                        known = true;
                        break;
                    }
                }
                if (!known) {
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
}
