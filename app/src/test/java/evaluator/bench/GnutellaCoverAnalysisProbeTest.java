package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.bench.BenchTypes.CountResult;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.decomposition.Decomposer;
import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.ExecutablePlan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.Planner;
import evaluator.evaluation.Relation;
import evaluator.evaluation.SystemRScorer;
import evaluator.index.CpqIndex;
import evaluator.index.NativeCpqIndex;
import evaluator.util.Deadline;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Manual probe to inspect exact-cover candidate pools, selected decompositions,
 * and prefix/intermediate result sizes on a small Gnutella workload slice.
 */
class GnutellaCoverAnalysisProbeTest {
    private static final String ENABLE_PROPERTY = "cpq.coverAnalysis.enabled";
    private static final String INDEX_PROPERTY = "cpq.coverAnalysis.index";
    private static final String METADATA_PROPERTY = "cpq.coverAnalysis.metadata";
    private static final String OUTPUT_DIR_PROPERTY = "cpq.coverAnalysis.outputDir";
    private static final String IDS_PROPERTY = "cpq.coverAnalysis.ids";
    private static final String K_PROPERTY = "cpq.coverAnalysis.k";
    private static final String COVER_LIMIT_PROPERTY = "cpq.coverAnalysis.coverLimit";
    private static final String DECOMPOSITION_TIMEOUT_PROPERTY = "cpq.coverAnalysis.decompositionTimeoutMs";
    private static final String EXEC_TIMEOUT_PROPERTY = "cpq.coverAnalysis.execTimeoutMs";
    private static final String TOP_COST_PROPERTY = "cpq.coverAnalysis.topCost";
    private static final String TOP_SCORE_PROPERTY = "cpq.coverAnalysis.topScore";

    private static final Path DEFAULT_INDEX = Path.of("local/indices/fork_p2p-Gnutella31.idx");
    private static final Path DEFAULT_METADATA = Path.of(
            "local/tmp/gnutella_estimator_cover_only_subset_20260320/gnutella100.metadata.csv");
    private static final Path DEFAULT_OUTPUT_DIR = Path.of("logs/gnutella_cover_analysis_20260324");
    private static final String DEFAULT_IDS = "49,4,46,35";
    private static final int DEFAULT_K = 2;
    private static final int DEFAULT_COVER_LIMIT = 256;
    private static final int DEFAULT_DECOMPOSITION_TIMEOUT_MS = 5000;
    private static final int DEFAULT_EXEC_TIMEOUT_MS = 5000;
    private static final int DEFAULT_TOP_COST = 5;
    private static final int DEFAULT_TOP_SCORE = 5;

    @Test
    void analyzeRepresentativeExactCoverCandidates() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean(ENABLE_PROPERTY), "Manual probe disabled");

        Path indexPath = Path.of(System.getProperty(INDEX_PROPERTY, DEFAULT_INDEX.toString()));
        Path metadataPath = Path.of(System.getProperty(METADATA_PROPERTY, DEFAULT_METADATA.toString()));
        Path outputDir = Path.of(System.getProperty(OUTPUT_DIR_PROPERTY, DEFAULT_OUTPUT_DIR.toString()));
        int k = Integer.getInteger(K_PROPERTY, DEFAULT_K);
        int coverLimit = Integer.getInteger(COVER_LIMIT_PROPERTY, DEFAULT_COVER_LIMIT);
        int decompositionTimeoutMs = Integer.getInteger(
                DECOMPOSITION_TIMEOUT_PROPERTY,
                DEFAULT_DECOMPOSITION_TIMEOUT_MS);
        int executionTimeoutMs = Integer.getInteger(
                EXEC_TIMEOUT_PROPERTY,
                DEFAULT_EXEC_TIMEOUT_MS);
        int topCost = Integer.getInteger(TOP_COST_PROPERTY, DEFAULT_TOP_COST);
        int topScore = Integer.getInteger(TOP_SCORE_PROPERTY, DEFAULT_TOP_SCORE);
        List<Integer> queryIds = parseIds(System.getProperty(IDS_PROPERTY, DEFAULT_IDS));

        Assumptions.assumeTrue(Files.exists(indexPath), "Missing index file " + indexPath);
        Assumptions.assumeTrue(Files.exists(metadataPath), "Missing metadata file " + metadataPath);

        Map<Integer, QuerySpec> metadata = loadMetadata(metadataPath);
        List<QuerySpec> queries = new ArrayList<>(queryIds.size());
        for (int queryId : queryIds) {
            QuerySpec spec = metadata.get(queryId);
            Assumptions.assumeTrue(spec != null, "Missing query id " + queryId + " in " + metadataPath);
            queries.add(spec);
        }

        NativeCpqIndex index = NativeCpqIndex.load(indexPath);
        if (k > index.k()) {
            throw new IllegalArgumentException(
                    "Requested k=" + k + " exceeds index k=" + index.k() + " for " + indexPath);
        }

        EngineConfig config = EngineConfig.defaults();
        Planner planner = new Planner(index, config.estimationSeed(), config.systemRMaxCandidateOrders());
        EstimatorDiagnostics diagnostics = new EstimatorDiagnostics(planner, config);
        SystemRScorer scorer = new SystemRScorer(index, config.systemRMaxCandidateOrders());

        Files.createDirectories(outputDir);
        Path candidatePath = outputDir.resolve("candidate_summary.csv");
        Path prefixPath = outputDir.resolve("prefix_trace.csv");
        Path summaryPath = outputDir.resolve("run_summary.txt");

        System.out.println("gnutella_cover_analysis_start");
        System.out.println("index=" + indexPath.toAbsolutePath());
        System.out.println("metadata=" + metadataPath.toAbsolutePath());
        System.out.println("output_dir=" + outputDir.toAbsolutePath());
        System.out.println("query_ids=" + queryIds);
        System.out.println("index_k=" + index.k());
        System.out.println("requested_k=" + k);
        System.out.println("cover_limit=" + coverLimit);
        System.out.println("decomposition_timeout_ms=" + decompositionTimeoutMs);
        System.out.println("execution_timeout_ms=" + executionTimeoutMs);
        System.out.println("top_cost=" + topCost);
        System.out.println("top_score=" + topScore);

        int candidateRows = 0;
        int prefixRows = 0;
        try (BufferedWriter candidateWriter = Files.newBufferedWriter(candidatePath, StandardCharsets.UTF_8);
                BufferedWriter prefixWriter = Files.newBufferedWriter(prefixPath, StandardCharsets.UTF_8);
                BufferedWriter summaryWriter = Files.newBufferedWriter(summaryPath, StandardCharsets.UTF_8)) {
            candidateWriter.write(
                    "query_id,family,template,stratum,pool_kind,pool_candidate_count,pool_hit_limit,plan_signature,"
                            + "selected_by_cost,selected_by_cover_score,reasons,cost_rank,score_rank,components,max_diameter,"
                            + "component_counts,component_signatures,variable_order,systemr_score,systemr_estimated_count,"
                            + "final_count,query_ms,mapping_ms,join_order_ms,join_ms,execution_ms,status,error");
            candidateWriter.newLine();

            prefixWriter.write(
                    "query_id,plan_signature,step,variable,prefix_order,activated_relations,"
                            + "estimated_prefix_count,extensible_prefix_actual_count,active_prefix_actual_count,"
                            + "estimate_ms,extensible_eval_ms,q_error,relative_error");
            prefixWriter.newLine();

            summaryWriter.write("started=" + Instant.now());
            summaryWriter.newLine();
            summaryWriter.write("index=" + indexPath.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("metadata=" + metadataPath.toAbsolutePath());
            summaryWriter.newLine();
            summaryWriter.write("query_ids=" + queryIds);
            summaryWriter.newLine();
            summaryWriter.write("cover_limit=" + coverLimit);
            summaryWriter.newLine();
            summaryWriter.write("decomposition_timeout_ms=" + decompositionTimeoutMs);
            summaryWriter.newLine();
            summaryWriter.write("execution_timeout_ms=" + executionTimeoutMs);
            summaryWriter.newLine();
            summaryWriter.write("top_cost=" + topCost);
            summaryWriter.newLine();
            summaryWriter.write("top_score=" + topScore);
            summaryWriter.newLine();
            summaryWriter.newLine();

            for (QuerySpec query : queries) {
                QueryAnalysis analysis = analyzeQuery(
                        query,
                        index,
                        planner,
                        config,
                        diagnostics,
                        scorer,
                        k,
                        coverLimit,
                        decompositionTimeoutMs,
                        executionTimeoutMs,
                        topCost,
                        topScore);
                summaryWriter.write(String.format(
                        Locale.ROOT,
                        "query_id=%d family=%s template=%s stratum=%s cost_candidates=%d neutral_candidates=%d cost_hit_limit=%s neutral_hit_limit=%s analyzed_candidates=%d%n",
                        query.id(),
                        query.family(),
                        query.template(),
                        query.stratum(),
                        analysis.costCandidateCount(),
                        analysis.neutralCandidateCount(),
                        analysis.costHitLimit(),
                        analysis.neutralHitLimit(),
                        analysis.analyzedCandidates().size()));
                for (AnalyzedCandidate candidate : analysis.analyzedCandidates()) {
                    writeCandidateRow(candidateWriter, query, candidate, analysis);
                    candidateRows++;
                    for (PrefixRow prefix : candidate.prefixRows()) {
                        writePrefixRow(prefixWriter, query, candidate, prefix);
                        prefixRows++;
                    }
                }
                summaryWriter.newLine();
            }
        }

        System.out.println("gnutella_cover_analysis_complete");
        System.out.println("candidate_rows=" + candidateRows);
        System.out.println("prefix_rows=" + prefixRows);
        System.out.println("candidate_summary=" + candidatePath.toAbsolutePath());
        System.out.println("prefix_trace=" + prefixPath.toAbsolutePath());
        System.out.println("run_summary=" + summaryPath.toAbsolutePath());
        assertTrue(candidateRows > 0, "Expected at least one analyzed candidate");
    }

    private static QueryAnalysis analyzeQuery(
            QuerySpec query,
            CpqIndex index,
            Planner planner,
            EngineConfig config,
            EstimatorDiagnostics diagnostics,
            SystemRScorer scorer,
            int k,
            int coverLimit,
            int decompositionTimeoutMs,
            int executionTimeoutMs,
            int topCost,
            int topScore) {
        ConjunctiveQuery cq = ConjunctiveQuery.parse(query.queryText());
        Planner.MethodSelection costSelection = planner.planMethod(
                cq,
                DecompositionMethod.COST,
                coverLimit,
                k,
                decompositionTimeoutMs,
                decompositionTimeoutMs);
        List<Planner.Candidate> costCandidates = costSelection.candidates();
        List<Plan> neutralPlans = collectNeutralPlans(cq, index, k, coverLimit, decompositionTimeoutMs);
        List<ScoredPlan> scoredNeutral = scorePlans(neutralPlans, scorer, executionTimeoutMs);

        String costSelectedSignature = costCandidates.isEmpty()
                ? null
                : planSignature(costCandidates.get(0).plan());
        String coverSelectedSignature = scoredNeutral.isEmpty()
                ? null
                : planSignature(scoredNeutral.get(0).plan());

        Map<String, Integer> costRankBySignature = new HashMap<>();
        for (int i = 0; i < costCandidates.size(); i++) {
            costRankBySignature.put(planSignature(costCandidates.get(i).plan()), i);
        }

        Map<String, Integer> scoreRankBySignature = new HashMap<>();
        for (int i = 0; i < scoredNeutral.size(); i++) {
            scoreRankBySignature.put(planSignature(scoredNeutral.get(i).plan()), i);
        }

        Map<String, CandidateSeed> subset = new LinkedHashMap<>();
        if (!costCandidates.isEmpty()) {
            addSeed(subset, costCandidates.get(0).plan(), "selected_cost");
        }
        if (!scoredNeutral.isEmpty()) {
            addSeed(subset, scoredNeutral.get(0).plan(), "selected_cover_score");
        }
        for (int i = 0; i < Math.min(topCost, costCandidates.size()); i++) {
            addSeed(subset, costCandidates.get(i).plan(), "cost_top_" + i);
        }
        for (int i = 0; i < Math.min(topScore, scoredNeutral.size()); i++) {
            addSeed(subset, scoredNeutral.get(i).plan(), "score_top_" + i);
        }

        List<AnalyzedCandidate> analyzed = new ArrayList<>(subset.size());
        for (CandidateSeed seed : subset.values()) {
            Plan plan = seed.plan();
            String signature = planSignature(plan);
            ScoredPlan score = findScore(scoredNeutral, signature);
            analyzed.add(analyzeCandidate(
                    query,
                    plan,
                    seed.reasons(),
                    costRankBySignature.get(signature),
                    scoreRankBySignature.get(signature),
                    Objects.equals(signature, costSelectedSignature),
                    Objects.equals(signature, coverSelectedSignature),
                    score,
                    planner,
                    index,
                    config,
                    diagnostics,
                    executionTimeoutMs));
        }
        analyzed.sort(Comparator
                .comparingInt((AnalyzedCandidate candidate) -> candidate.costRank() == null
                        ? Integer.MAX_VALUE
                        : candidate.costRank())
                .thenComparing(candidate -> candidate.planSignature()));

        return new QueryAnalysis(
                query,
                costCandidates.size(),
                neutralPlans.size(),
                costCandidates.size() >= coverLimit,
                neutralPlans.size() >= coverLimit,
                List.copyOf(analyzed));
    }

    private static AnalyzedCandidate analyzeCandidate(
            QuerySpec query,
            Plan plan,
            Set<String> reasons,
            Integer costRank,
            Integer scoreRank,
            boolean selectedByCost,
            boolean selectedByCoverScore,
            ScoredPlan score,
            Planner planner,
            CpqIndex index,
            EngineConfig config,
            EstimatorDiagnostics diagnostics,
            int executionTimeoutMs) {
        long deadlineNanos = deadlineAfterMs(executionTimeoutMs);
        String signature = planSignature(plan);
        try {
            ExecutablePlan executable = ExecutablePlan.compile(plan, index, deadlineNanos);
            BenchTypes.EvaluationStats stats = new BenchTypes.EvaluationStats();
            stats.addQueryNanos(executable.compilationStats().queryNanos());
            stats.addMappingNanos(executable.compilationStats().mappingNanos());
            if (executable.isEmpty()) {
                return new AnalyzedCandidate(
                        signature,
                        plan,
                        reasons,
                        costRank,
                        scoreRank,
                        selectedByCost,
                        selectedByCoverScore,
                        score == null ? Double.NaN : score.score(),
                        score == null ? Double.NaN : score.estimatedCount(),
                        List.of(),
                        List.of(),
                        0L,
                        stats.queryNanos(),
                        stats.mappingNanos(),
                        0L,
                        0L,
                        stats.queryNanos() + stats.mappingNanos(),
                        "EMPTY",
                        null);
            }

            Planner.JoinOrderPlan orderPlan = planner.selectJoinOrder(
                    executable,
                    config.estimateHeuristicJoinOrders(),
                    deadlineNanos);
            stats.addEstimateNanos(orderPlan.estimateNanos());
            long joinStart = System.nanoTime();
            LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                    orderPlan.order(),
                    LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                    config.joinSafeDistinctFastPath(),
                    deadlineNanos);
            long joinNanos = System.nanoTime() - joinStart;
            stats.addJoinNanos(joinNanos);

            List<EstimatorDiagnostics.PrefixEstimationStep> traced = diagnostics.tracePrefixEstimationSteps(
                    executable,
                    orderPlan.order(),
                    deadlineAfterMs(executionTimeoutMs));
            List<PrefixRow> prefixRows = buildPrefixRows(
                    executable,
                    orderPlan.order(),
                    traced,
                    config.joinSafeDistinctFastPath(),
                    executionTimeoutMs);
            return new AnalyzedCandidate(
                    signature,
                    plan,
                    reasons,
                    costRank,
                    scoreRank,
                    selectedByCost,
                    selectedByCoverScore,
                    score == null ? Double.NaN : score.score(),
                    score == null ? orderPlan.estimatedCount() : score.estimatedCount(),
                    List.copyOf(executable.componentCounts()),
                    orderPlan.order(),
                    count.count(),
                    stats.queryNanos(),
                    stats.mappingNanos(),
                    stats.estimateNanos(),
                    stats.joinNanos(),
                    stats.queryNanos() + stats.mappingNanos() + stats.estimateNanos() + stats.joinNanos(),
                    "OK",
                    null,
                    List.copyOf(prefixRows));
        } catch (Deadline.Exceeded | CancellationException ex) {
            return new AnalyzedCandidate(
                    signature,
                    plan,
                    reasons,
                    costRank,
                    scoreRank,
                    selectedByCost,
                    selectedByCoverScore,
                    score == null ? Double.NaN : score.score(),
                    score == null ? Double.NaN : score.estimatedCount(),
                    List.of(),
                    List.of(),
                    -1L,
                    -1L,
                    -1L,
                    -1L,
                    -1L,
                    -1L,
                    "EXEC_TIMEOUT",
                    ex.getClass().getSimpleName(),
                    List.of());
        }
    }

    private static List<PrefixRow> buildPrefixRows(
            ExecutablePlan executable,
            List<String> order,
            List<EstimatorDiagnostics.PrefixEstimationStep> traced,
            boolean safeDistinctFastPath,
            int executionTimeoutMs) {
        List<Long> activeActuals = activePrefixCounts(executable, order, safeDistinctFastPath, executionTimeoutMs);
        List<Integer> activatedRelations = activatedRelationCounts(executable, order);
        List<PrefixRow> rows = new ArrayList<>(traced.size());
        for (int i = 0; i < traced.size(); i++) {
            EstimatorDiagnostics.PrefixEstimationStep step = traced.get(i);
            long activeActual = i < activeActuals.size() ? activeActuals.get(i) : -1L;
            int activated = i < activatedRelations.size() ? activatedRelations.get(i) : 0;
            rows.add(new PrefixRow(
                    step.step(),
                    step.variable(),
                    step.prefixOrder(),
                    activated,
                    step.estimate(),
                    step.actual(),
                    activeActual,
                    step.estimateNanos(),
                    step.evalNanos(),
                    step.qError(),
                    step.relativeError()));
        }
        return List.copyOf(rows);
    }

    private static List<Long> activePrefixCounts(
            ExecutablePlan executable,
            List<String> order,
            boolean safeDistinctFastPath,
            int executionTimeoutMs) {
        List<Long> counts = new ArrayList<>(order.size());
        List<Relation> relations = executable.relations();
        Map<String, Integer> positionByVariable = new HashMap<>(order.size());
        for (int i = 0; i < order.size(); i++) {
            positionByVariable.put(order.get(i), i);
        }
        int[] activationDepths = new int[relations.size()];
        for (int i = 0; i < relations.size(); i++) {
            int activation = -1;
            for (String variable : relations.get(i).variables()) {
                activation = Math.max(activation, positionByVariable.getOrDefault(variable, -1));
            }
            activationDepths[i] = activation;
        }
        for (int depth = 0; depth < order.size(); depth++) {
            List<String> prefix = List.copyOf(order.subList(0, depth + 1));
            List<Relation> active = new ArrayList<>();
            for (int i = 0; i < relations.size(); i++) {
                if (activationDepths[i] >= 0 && activationDepths[i] <= depth) {
                    active.add(relations.get(i));
                }
            }
            if (active.isEmpty()) {
                counts.add(-1L);
                continue;
            }
            long deadlineNanos = deadlineAfterMs(executionTimeoutMs);
            try {
                LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) LeapfrogJoin.join(
                        active,
                        prefix,
                        prefix,
                        LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                        safeDistinctFastPath,
                        deadlineNanos);
                counts.add(count.count());
            } catch (Deadline.Exceeded | CancellationException ex) {
                counts.add(-1L);
            }
        }
        return List.copyOf(counts);
    }

    private static List<Integer> activatedRelationCounts(ExecutablePlan executable, List<String> order) {
        List<Integer> counts = new ArrayList<>(order.size());
        Map<String, Integer> positionByVariable = new HashMap<>(order.size());
        for (int i = 0; i < order.size(); i++) {
            positionByVariable.put(order.get(i), i);
        }
        int[] activationDepths = new int[executable.relations().size()];
        for (int i = 0; i < executable.relations().size(); i++) {
            int activation = -1;
            for (String variable : executable.relations().get(i).variables()) {
                activation = Math.max(activation, positionByVariable.getOrDefault(variable, -1));
            }
            activationDepths[i] = activation;
        }
        for (int depth = 0; depth < order.size(); depth++) {
            int active = 0;
            for (int activationDepth : activationDepths) {
                if (activationDepth >= 0 && activationDepth <= depth) {
                    active++;
                }
            }
            counts.add(active);
        }
        return List.copyOf(counts);
    }

    private static List<ScoredPlan> scorePlans(
            List<Plan> plans,
            SystemRScorer scorer,
            int executionTimeoutMs) {
        List<ScoredPlan> scored = new ArrayList<>(plans.size());
        for (Plan plan : plans) {
            long deadlineNanos = deadlineAfterMs(executionTimeoutMs);
            try {
                SystemRScorer.PlanSelection selection = scorer.scorePlanWithHeuristicOrder(plan, deadlineNanos);
                scored.add(new ScoredPlan(
                        plan,
                        selection.score(),
                        selection.estimatedCount(),
                        selection.order()));
            } catch (Deadline.Exceeded | CancellationException ex) {
                scored.add(new ScoredPlan(plan, Double.POSITIVE_INFINITY, Double.NaN, List.of()));
            }
        }
        scored.sort(Comparator
                .comparingDouble(ScoredPlan::score)
                .thenComparingInt(scoredPlan -> scoredPlan.plan().components().size())
                .thenComparingInt(scoredPlan -> scoredPlan.plan().maxDiameter())
                .thenComparing(scoredPlan -> planSignature(scoredPlan.plan())));
        return List.copyOf(scored);
    }

    private static ScoredPlan findScore(List<ScoredPlan> scored, String signature) {
        for (ScoredPlan candidate : scored) {
            if (planSignature(candidate.plan()).equals(signature)) {
                return candidate;
            }
        }
        return null;
    }

    private static List<Plan> collectNeutralPlans(
            ConjunctiveQuery cq,
            CpqIndex index,
            int k,
            int coverLimit,
            int decompositionTimeoutMs) {
        long deadlineNanos = deadlineAfterMs(decompositionTimeoutMs);
        return Decomposer.cpqkCoverCost(k, coverLimit, cpq -> 0L, index::supports, deadlineNanos)
                .decompose(cq.syntax())
                .toList();
    }

    private static void addSeed(Map<String, CandidateSeed> subset, Plan plan, String reason) {
        String signature = planSignature(plan);
        CandidateSeed existing = subset.get(signature);
        if (existing == null) {
            LinkedHashSet<String> reasons = new LinkedHashSet<>();
            reasons.add(reason);
            subset.put(signature, new CandidateSeed(plan, reasons));
        } else {
            existing.reasons().add(reason);
        }
    }

    private static String planSignature(Plan plan) {
        List<String> signatures = new ArrayList<>(plan.components().size());
        for (Plan.Component component : plan.components()) {
            signatures.add(component.signature());
        }
        signatures.sort(String::compareTo);
        return String.join("|", signatures);
    }

    private static String componentSignatures(Plan plan) {
        List<String> signatures = new ArrayList<>(plan.components().size());
        for (Plan.Component component : plan.components()) {
            signatures.add(component.signature());
        }
        return String.join(" || ", signatures);
    }

    private static void writeCandidateRow(
            BufferedWriter writer,
            QuerySpec query,
            AnalyzedCandidate candidate,
            QueryAnalysis analysis) throws Exception {
        writer.write(String.format(
                Locale.ROOT,
                "%d,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%.6f,%.6f,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%s,%s",
                query.id(),
                csv(query.family()),
                csv(query.template()),
                csv(query.stratum()),
                csv("exact_cover"),
                analysis.neutralCandidateCount(),
                analysis.neutralHitLimit(),
                csv(candidate.planSignature()),
                candidate.selectedByCost(),
                candidate.selectedByCoverScore(),
                csv(String.join("|", candidate.reasons())),
                csv(candidate.costRank() == null ? "-" : Integer.toString(candidate.costRank())),
                csv(candidate.scoreRank() == null ? "-" : Integer.toString(candidate.scoreRank())),
                candidate.plan().size(),
                candidate.plan().maxDiameter(),
                csv(joinLongs(candidate.componentCounts())),
                csv(componentSignatures(candidate.plan())),
                csv(formatOrder(candidate.variableOrder())),
                candidate.systemRScore(),
                candidate.systemREstimatedCount(),
                candidate.finalCount(),
                nanosToMillis(candidate.queryNanos()),
                nanosToMillis(candidate.mappingNanos()),
                nanosToMillis(candidate.joinOrderNanos()),
                nanosToMillis(candidate.joinNanos()),
                nanosToMillis(candidate.executionNanos()),
                csv(candidate.status()),
                csv(candidate.error() == null ? "-" : candidate.error())));
        writer.newLine();
    }

    private static void writePrefixRow(
            BufferedWriter writer,
            QuerySpec query,
            AnalyzedCandidate candidate,
            PrefixRow prefix) throws Exception {
        writer.write(String.format(
                Locale.ROOT,
                "%d,%s,%d,%s,%s,%d,%.6f,%d,%d,%.3f,%.3f,%.6f,%.6f",
                query.id(),
                csv(candidate.planSignature()),
                prefix.step(),
                csv(prefix.variable()),
                csv(formatOrder(prefix.prefixOrder())),
                prefix.activatedRelations(),
                prefix.estimatedPrefixCount(),
                prefix.extensiblePrefixActualCount(),
                prefix.activePrefixActualCount(),
                nanosToMillis(prefix.estimateNanos()),
                nanosToMillis(prefix.extensibleEvalNanos()),
                prefix.qError(),
                prefix.relativeError()));
        writer.newLine();
    }

    private static double nanosToMillis(long nanos) {
        if (nanos < 0L) {
            return -1.0D;
        }
        return nanos / 1_000_000.0D;
    }

    private static long deadlineAfterMs(int timeoutMs) {
        if (timeoutMs <= 0) {
            return Long.MAX_VALUE;
        }
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        return deadline < 0L ? Long.MAX_VALUE : deadline;
    }

    private static String formatOrder(List<String> order) {
        return order == null || order.isEmpty() ? "[]" : "[" + String.join(",", order) + "]";
    }

    private static String joinLongs(List<Long> values) {
        if (values == null || values.isEmpty()) {
            return "-";
        }
        List<String> formatted = new ArrayList<>(values.size());
        for (long value : values) {
            formatted.add(Long.toString(value));
        }
        return String.join("|", formatted);
    }

    private static List<Integer> parseIds(String raw) {
        List<Integer> ids = new ArrayList<>();
        for (String part : raw.split(",")) {
            String token = part.trim();
            if (!token.isEmpty()) {
                ids.add(Integer.parseInt(token));
            }
        }
        return List.copyOf(ids);
    }

    private static Map<Integer, QuerySpec> loadMetadata(Path metadataPath) throws Exception {
        List<String> lines = Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            return Map.of();
        }
        String[] header = splitCsv(lines.get(0));
        Map<String, Integer> indexByColumn = new HashMap<>(header.length);
        for (int i = 0; i < header.length; i++) {
            indexByColumn.put(header[i], i);
        }

        Map<Integer, QuerySpec> rows = new LinkedHashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] columns = splitCsv(line);
            int id = Integer.parseInt(unquote(columns[indexByColumn.get("subset_order")]));
            rows.put(id, new QuerySpec(
                    id,
                    unquote(columns[indexByColumn.get("family")]),
                    unquote(columns[indexByColumn.get("template")]),
                    unquote(columns[indexByColumn.get("stratum")]),
                    unquote(columns[indexByColumn.get("query_text")])));
        }
        return Map.copyOf(rows);
    }

    private static String[] splitCsv(String line) {
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            return trimmed.substring(1, trimmed.length() - 1).replace("\"\"", "\"");
        }
        return trimmed;
    }

    private static String csv(String value) {
        String safe = value == null ? "" : value.replace("\"", "'");
        return "\"" + safe + "\"";
    }

    private record QuerySpec(
            int id,
            String family,
            String template,
            String stratum,
            String queryText) {
    }

    private record QueryAnalysis(
            QuerySpec query,
            int costCandidateCount,
            int neutralCandidateCount,
            boolean costHitLimit,
            boolean neutralHitLimit,
            List<AnalyzedCandidate> analyzedCandidates) {
    }

    private record CandidateSeed(Plan plan, LinkedHashSet<String> reasons) {
    }

    private record ScoredPlan(
            Plan plan,
            double score,
            double estimatedCount,
            List<String> order) {
    }

    private record PrefixRow(
            int step,
            String variable,
            List<String> prefixOrder,
            int activatedRelations,
            double estimatedPrefixCount,
            long extensiblePrefixActualCount,
            long activePrefixActualCount,
            long estimateNanos,
            long extensibleEvalNanos,
            double qError,
            double relativeError) {
    }

    private record AnalyzedCandidate(
            String planSignature,
            Plan plan,
            Set<String> reasons,
            Integer costRank,
            Integer scoreRank,
            boolean selectedByCost,
            boolean selectedByCoverScore,
            double systemRScore,
            double systemREstimatedCount,
            List<Long> componentCounts,
            List<String> variableOrder,
            long finalCount,
            long queryNanos,
            long mappingNanos,
            long joinOrderNanos,
            long joinNanos,
            long executionNanos,
            String status,
            String error,
            List<PrefixRow> prefixRows) {
        private AnalyzedCandidate(
                String planSignature,
                Plan plan,
                Set<String> reasons,
                Integer costRank,
                Integer scoreRank,
                boolean selectedByCost,
                boolean selectedByCoverScore,
                double systemRScore,
                double systemREstimatedCount,
                List<Long> componentCounts,
                List<String> variableOrder,
                long finalCount,
                long queryNanos,
                long mappingNanos,
                long joinOrderNanos,
                long joinNanos,
                long executionNanos,
                String status,
                String error) {
            this(
                    planSignature,
                    plan,
                    Set.copyOf(reasons),
                    costRank,
                    scoreRank,
                    selectedByCost,
                    selectedByCoverScore,
                    systemRScore,
                    systemREstimatedCount,
                    List.copyOf(componentCounts),
                    List.copyOf(variableOrder),
                    finalCount,
                    queryNanos,
                    mappingNanos,
                    joinOrderNanos,
                    joinNanos,
                    executionNanos,
                    status,
                    error,
                    List.of());
        }
    }
}
