package evaluator.evaluation;

import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.index.CpqIndex;
import evaluator.index.CpqIndex.Endpoint;
import evaluator.index.CpqIndex.RelationSynopsis;
import evaluator.util.Deadline;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic System R-style scorer over compact planner-side summaries.
 * <p>
 * Planning keeps only arithmetic synopses for relations and intermediate
 * results: tuple cardinality and one NDV estimate per exposed variable.
 */
public final class SystemRScorer {
    private final CpqIndex index;
    private final int maxCandidateOrders;

    public SystemRScorer(CpqIndex index) {
        this(index, 0);
    }

    public SystemRScorer(CpqIndex index, int maxCandidateOrders) {
        this.index = Objects.requireNonNull(index, "index");
        this.maxCandidateOrders = Math.max(0, maxCandidateOrders);
    }

    /**
     * Selects the best candidate plan from one candidate family using a shared
     * per-component synopsis cache.
     */
    public PlanSelection selectBestPlan(List<Plan> candidates, long deadlineNanos) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        Map<String, RelationSummary> componentCache = new HashMap<>();
        PlanSelection best = null;
        for (Plan candidate : candidates) {
            Deadline.check(deadlineNanos);
            PlanSelection current = scorePlanWithHeuristicOrder(candidate, componentCache, deadlineNanos);
            if (best == null || comparePlans(current, best) < 0) {
                best = current;
            }
        }
        return best;
    }

    /**
     * Scores one plan under the default heuristic order used by the baseline
     * execution policy.
     */
    public PlanSelection scorePlanWithHeuristicOrder(Plan plan, long deadlineNanos) {
        Objects.requireNonNull(plan, "plan");
        return scorePlanWithHeuristicOrder(plan, new HashMap<>(), deadlineNanos);
    }

    /**
     * Selects the best variable order for a plan using compact planner-side
     * synopses.
     */
    public OrderSelection selectBestOrder(Plan plan, long deadlineNanos) {
        Objects.requireNonNull(plan, "plan");
        return selectBestOrder(compilePlan(plan, new HashMap<>(), deadlineNanos), deadlineNanos);
    }

    /**
     * Selects the best variable order for an already compiled executable plan.
     * Order refinement starts from the baseline heuristic order and evaluates a
     * small local neighborhood using the materialized evaluator relations.
     */
    public OrderSelection selectBestOrder(ExecutablePlan executable, long deadlineNanos) {
        Objects.requireNonNull(executable, "executable");
        StatsPlan compiled = compileExecutable(executable);
        List<String> baseOrder = defaultOrder(compiled.plan(), compiled.componentCounts());
        return selectBestLocalOrder(executable, baseOrder, deadlineNanos);
    }

    /**
     * Selects the best order from a small neighborhood around a supplied base
     * order using the already materialized execution-time relations.
     */
    public OrderSelection selectBestLocalOrder(
            ExecutablePlan executable,
            List<String> baseOrder,
            long deadlineNanos) {
        Objects.requireNonNull(executable, "executable");
        Objects.requireNonNull(baseOrder, "baseOrder");
        StatsPlan compiled = compileExecutable(executable);
        if (compiled.empty()) {
            return new OrderSelection(defaultOrder(compiled.plan(), compiled.componentCounts()), 0.0D, 0.0D);
        }

        List<String> normalizedBase = normalizeOrder(baseOrder, compiled.relations());
        List<List<String>> candidateOrders = OrderCandidates.buildLocal(
                normalizedBase,
                compiled.plan().projectedVariableNames(),
                maxCandidateOrders);
        return selectBestLocalOrder(compiled, candidateOrders, normalizedBase, deadlineNanos);
    }

    /**
     * Estimates projected cardinality for a plan and fixed variable order using
     * compact planner-side synopses.
     */
    public ProjectedCountEstimate estimateProjectedCount(
            Plan plan,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(projected, "projected");
        StatsPlan compiled = compilePlan(plan, new HashMap<>(), deadlineNanos);
        if (compiled.empty()) {
            return new ProjectedCountEstimate(0.0D);
        }
        double estimate = estimateProjectedCardinality(compiled.relations(), variableOrder, projected, deadlineNanos);
        return new ProjectedCountEstimate(estimate);
    }

    /**
     * Estimates projected cardinality for already materialized relations using
     * the same summary propagation logic.
     */
    public ProjectedCountEstimate estimateProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        Objects.requireNonNull(relations, "relations");
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(projected, "projected");
        if (relations.isEmpty()) {
            return new ProjectedCountEstimate(0.0D);
        }
        List<RelationSummary> summaries = relationSummaries(relations);
        double estimate = estimateProjectedCardinality(summaries, variableOrder, projected, deadlineNanos);
        return new ProjectedCountEstimate(estimate);
    }

    private OrderSelection selectBestOrder(StatsPlan compiled, long deadlineNanos) {
        if (compiled.empty()) {
            return new OrderSelection(defaultOrder(compiled.plan(), compiled.componentCounts()), 0.0D, 0.0D);
        }
        List<String> baseOrder = defaultOrder(compiled.plan(), compiled.componentCounts());
        List<String> normalizedBase = normalizeOrder(baseOrder, compiled.relations());
        List<List<String>> candidateOrders = OrderCandidates.buildLocal(
                normalizedBase,
                compiled.plan().projectedVariableNames(),
                maxCandidateOrders);
        return selectBestLocalOrder(compiled, candidateOrders, normalizedBase, deadlineNanos);
    }

    private OrderSelection selectBestLocalOrder(
            StatsPlan compiled,
            List<List<String>> candidateOrders,
            List<String> baseOrder,
            long deadlineNanos) {
        RelationLayout layout = relationLayout(compiled.relations(), candidateOrders.get(0), deadlineNanos);
        int[] projectedIndexes = projectedIndexes(compiled.plan().projectedVariableNames(), layout);
        OrderSelection baseSelection = evaluateOrder(compiled, layout, projectedIndexes, baseOrder, deadlineNanos);
        OrderSelection best = baseSelection;
        for (List<String> order : candidateOrders) {
            Deadline.check(deadlineNanos);
            OrderSelection current = evaluateOrder(compiled, layout, projectedIndexes, order, deadlineNanos);
            if (compareOrders(current, best, compiled.plan().projectedVariableNames()) < 0) {
                best = current;
            }
        }
        return compareOrders(best, baseSelection, compiled.plan().projectedVariableNames()) < 0
                ? best
                : baseSelection;
    }

    private PlanSelection scorePlanWithHeuristicOrder(
            Plan plan,
            Map<String, RelationSummary> componentCache,
            long deadlineNanos) {
        StatsPlan compiled = compilePlan(plan, componentCache, deadlineNanos);
        if (compiled.empty()) {
            List<String> order = defaultOrder(plan, List.of());
            return new PlanSelection(plan, order, 0.0D, 0.0D);
        }
        List<String> heuristicOrder = defaultOrder(compiled.plan(), compiled.componentCounts());
        OrderSelection selection = evaluateOrderWithNormalizedBase(compiled, heuristicOrder, deadlineNanos);
        return new PlanSelection(plan, selection.order(), selection.score(), selection.estimatedCount());
    }

    private OrderSelection evaluateOrderWithNormalizedBase(
            StatsPlan compiled,
            List<String> order,
            long deadlineNanos) {
        List<String> normalizedOrder = normalizeOrder(order, compiled.relations());
        RelationLayout layout = relationLayout(compiled.relations(), normalizedOrder, deadlineNanos);
        int[] projectedIndexes = projectedIndexes(compiled.plan().projectedVariableNames(), layout);
        return evaluateOrder(compiled, layout, projectedIndexes, normalizedOrder, deadlineNanos);
    }

    private StatsPlan compilePlan(
            Plan plan,
            Map<String, RelationSummary> componentCache,
            long deadlineNanos) {
        List<RelationSummary> relations = new ArrayList<>(plan.components().size());
        List<Long> counts = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            Deadline.check(deadlineNanos);
            String signature = component.signature();
            RelationSummary compiled = componentCache.get(signature);
            if (compiled == null) {
                compiled = compileComponent(component);
                componentCache.put(signature, compiled);
            }
            if (compiled.empty()) {
                return new StatsPlan(plan, List.of(), List.of(), true);
            }
            relations.add(compiled);
            counts.add((long) compiled.cardinality());
        }
        return new StatsPlan(plan, List.copyOf(relations), List.copyOf(counts), false);
    }

    private static StatsPlan compileExecutable(ExecutablePlan executable) {
        if (executable.isEmpty()) {
            return new StatsPlan(executable.plan(), List.of(), List.of(), true);
        }
        List<RelationSummary> relations = relationSummaries(executable.relations());
        boolean empty = false;
        for (RelationSummary relation : relations) {
            if (relation.empty()) {
                empty = true;
                break;
            }
        }
        return new StatsPlan(
                executable.plan(),
                List.copyOf(relations),
                executable.componentCounts(),
                empty);
    }

    private RelationSummary compileComponent(Component component) {
        RelationSynopsis synopsis = index.componentSynopsis(component.cpq());
        return RelationSummary.from(component, synopsis);
    }

    private OrderSelection evaluateOrder(
            StatsPlan compiled,
            RelationLayout layout,
            int[] projectedIndexes,
            List<String> variableOrder,
            long deadlineNanos) {
        OrderContext context = orderContext(layout, variableOrder, deadlineNanos);
        OrderAccumulator accumulator = new OrderAccumulator(context.variableCount());

        double prefixSum = 0.0D;
        for (int depth = 0; depth < context.variableCount(); depth++) {
            Deadline.check(deadlineNanos);
            includeActivatedRelations(context, accumulator, depth, deadlineNanos);
            double prefixEstimate = accumulator.initialized()
                    ? projectPrefixCardinality(accumulator.summary(), context, depth + 1)
                    : boundaryPrefixEstimate(context, depth + 1);
            prefixSum = safeAdd(prefixSum, prefixEstimate);
        }

        double score = safeAdd(componentWork(compiled.componentCounts()), prefixSum);
        double estimatedCount = estimateProjectedCardinality(context, projectedIndexes, accumulator, deadlineNanos);
        return new OrderSelection(context.order(), score, estimatedCount);
    }

    private static double componentWork(List<Long> componentCounts) {
        double total = 0.0D;
        for (long count : componentCounts) {
            total = safeAdd(total, normalizeCardinality(count));
        }
        return total;
    }

    private static List<RelationSummary> relationSummaries(List<Relation> relations) {
        List<RelationSummary> infos = new ArrayList<>(relations.size());
        for (Relation relation : relations) {
            infos.add(RelationSummary.from(relation));
        }
        return List.copyOf(infos);
    }

    private static double estimateProjectedCardinality(
            List<RelationSummary> relations,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        if (relations.isEmpty()) {
            return 0.0D;
        }

        List<String> normalizedOrder = normalizeOrder(variableOrder, relations);
        RelationLayout layout = relationLayout(relations, normalizedOrder, deadlineNanos);
        OrderContext context = orderContext(layout, normalizedOrder, deadlineNanos);
        OrderAccumulator accumulator = new OrderAccumulator(context.variableCount());
        for (int depth = 0; depth < context.variableCount(); depth++) {
            Deadline.check(deadlineNanos);
            includeActivatedRelations(context, accumulator, depth, deadlineNanos);
        }
        return estimateProjectedCardinality(context, projectedIndexes(projected, layout), accumulator, deadlineNanos);
    }

    /**
     * Joins one summary state with one relation using only arithmetic NDV
     * propagation. Shared variables use inclusion-style overlap:
     * {@code min(ndvLeft, ndvRight)}.
     */
    private static SummaryState joinSummary(
            SummaryState current,
            RelationBinding relation) {
        double outputCardinality = safeMultiply(current.cardinality(), relation.cardinality());
        for (int i = 0; i < relation.variableCount(); i++) {
            int variable = relation.variableIndex(i);
            if (!current.contains(variable)) {
                continue;
            }
            double leftDistinct = current.distinctCount(variable);
            double rightDistinct = relation.distinctCount(i);
            double overlap = overlapDistinct(leftDistinct, rightDistinct);
            if (overlap <= 0.0D) {
                return SummaryState.empty(current.variableCount());
            }
            outputCardinality = safeMultiply(
                    outputCardinality,
                    overlap / safeMultiply(leftDistinct, rightDistinct));
        }
        if (outputCardinality <= 0.0D) {
            return SummaryState.empty(current.variableCount());
        }

        boolean[] present = new boolean[current.variableCount()];
        double[] distinctCounts = new double[current.variableCount()];
        double currentRetention = retention(outputCardinality, current.cardinality());
        double relationRetention = retention(outputCardinality, relation.cardinality());
        for (int variable = 0; variable < current.variableCount(); variable++) {
            boolean leftPresent = current.contains(variable);
            int rightIndex = relation.indexOf(variable);
            boolean rightPresent = rightIndex >= 0;
            if (!leftPresent && !rightPresent) {
                continue;
            }

            double distinct = 0.0D;
            if (leftPresent && rightPresent) {
                distinct = normalizeCardinality(Math.min(
                        outputCardinality,
                        overlapDistinct(current.distinctCount(variable), relation.distinctCount(rightIndex))));
            } else if (leftPresent) {
                distinct = shrinkDistinct(current.distinctCount(variable), currentRetention, outputCardinality);
            } else {
                distinct = shrinkDistinct(relation.distinctCount(rightIndex), relationRetention, outputCardinality);
            }
            if (distinct > 0.0D) {
                present[variable] = true;
                distinctCounts[variable] = distinct;
            }
        }
        return new SummaryState(outputCardinality, present, distinctCounts);
    }

    private static double overlapDistinct(double leftDistinct, double rightDistinct) {
        if (leftDistinct <= 0.0D || rightDistinct <= 0.0D) {
            return 0.0D;
        }
        return Math.min(leftDistinct, rightDistinct);
    }

    private static double retention(double outputCardinality, double inputCardinality) {
        if (outputCardinality <= 0.0D || inputCardinality <= 0.0D) {
            return 0.0D;
        }
        return Math.min(1.0D, outputCardinality / inputCardinality);
    }

    private static double shrinkDistinct(double distinctCount, double retention, double outputCardinality) {
        if (distinctCount <= 0.0D || outputCardinality <= 0.0D) {
            return 0.0D;
        }
        double retained = Math.min(distinctCount, distinctCount * Math.max(0.0D, retention));
        retained = Math.min(retained, outputCardinality);
        if (retained > 0.0D && retained < 1.0D) {
            return 1.0D;
        }
        return normalizeCardinality(retained);
    }

    private static double projectCardinality(
            SummaryState summary,
            int[] projectedIndexes) {
        if (summary.cardinality() <= 0.0D) {
            return 0.0D;
        }
        if (projectedIndexes.length == 0) {
            return normalizeCardinality(summary.cardinality());
        }

        double projectedProduct = 1.0D;
        for (int variable : projectedIndexes) {
            if (!summary.contains(variable)) {
                continue;
            }
            projectedProduct = safeMultiply(projectedProduct, summary.distinctCount(variable));
        }
        return normalizeCardinality(Math.min(summary.cardinality(), projectedProduct));
    }

    private static double projectPrefixCardinality(
            SummaryState summary,
            OrderContext context,
            int prefixLength) {
        if (summary.cardinality() <= 0.0D) {
            return 0.0D;
        }
        if (prefixLength <= 0) {
            return normalizeCardinality(summary.cardinality());
        }

        double projectedProduct = 1.0D;
        for (int depth = 0; depth < prefixLength; depth++) {
            int variable = context.variableAt(depth);
            if (!summary.contains(variable)) {
                continue;
            }
            projectedProduct = safeMultiply(projectedProduct, summary.distinctCount(variable));
        }
        return normalizeCardinality(Math.min(summary.cardinality(), projectedProduct));
    }

    private static double boundaryPrefixEstimate(OrderContext context, int prefixLength) {
        if (prefixLength <= 0) {
            return 0.0D;
        }

        double estimate = 1.0D;
        for (int depth = 0; depth < prefixLength; depth++) {
            int variable = context.variableAt(depth);
            if (!context.presentInPlan(variable)) {
                continue;
            }
            estimate = safeMultiply(estimate, context.boundaryDistinctCount(variable));
        }
        return normalizeCardinality(estimate);
    }

    private static double boundaryPrefixEstimate(OrderContext context, int[] projectedIndexes) {
        if (projectedIndexes.length == 0) {
            return 0.0D;
        }

        double estimate = 1.0D;
        for (int variable : projectedIndexes) {
            if (!context.presentInPlan(variable)) {
                continue;
            }
            estimate = safeMultiply(estimate, context.boundaryDistinctCount(variable));
        }
        return normalizeCardinality(estimate);
    }

    /**
     * Builds one reusable view of a variable order so scoring can update compact
     * intermediate summaries incrementally instead of rescoring the full prefix.
     */
    private static RelationLayout relationLayout(
            List<RelationSummary> relations,
            List<String> normalizedOrder,
            long deadlineNanos) {
        Map<String, Integer> variableIndexes = new HashMap<>(normalizedOrder.size());
        for (int i = 0; i < normalizedOrder.size(); i++) {
            variableIndexes.put(normalizedOrder.get(i), i);
        }

        double[] boundaryDistinctCounts = new double[normalizedOrder.size()];
        boolean[] presentInPlan = new boolean[normalizedOrder.size()];
        RelationBinding[] bindings = new RelationBinding[relations.size()];

        for (int i = 0; i < relations.size(); i++) {
            Deadline.check(deadlineNanos);
            RelationSummary relation = relations.get(i);
            List<String> variables = relation.variables();
            int[] indexes = new int[variables.size()];
            double[] distinctCounts = new double[variables.size()];
            for (int j = 0; j < variables.size(); j++) {
                String variable = variables.get(j);
                Integer index = variableIndexes.get(variable);
                if (index == null) {
                    throw new IllegalArgumentException("Variable order is missing relation variable: " + variable);
                }
                indexes[j] = index;
                distinctCounts[j] = relation.distinctCount(variable);
                presentInPlan[index] = true;
                boundaryDistinctCounts[index] = boundaryDistinctCounts[index] == 0.0D
                        ? distinctCounts[j]
                        : Math.min(boundaryDistinctCounts[index], distinctCounts[j]);
            }
            bindings[i] = new RelationBinding(relation.cardinality(), indexes, distinctCounts);
        }
        return new RelationLayout(
                List.copyOf(normalizedOrder),
                variableIndexes,
                bindings,
                boundaryDistinctCounts,
                presentInPlan);
    }

    private static OrderContext orderContext(
            RelationLayout layout,
            List<String> variableOrder,
            long deadlineNanos) {
        int variableCount = layout.variableCount();
        int[] orderVariables = new int[variableCount];
        boolean[] seen = new boolean[variableCount];
        int size = 0;
        for (String variable : variableOrder) {
            Deadline.check(deadlineNanos);
            Integer index = layout.variableIndex(variable);
            if (index != null && !seen[index]) {
                orderVariables[size++] = index;
                seen[index] = true;
            }
        }
        for (int index = 0; index < variableCount; index++) {
            if (!seen[index]) {
                orderVariables[size++] = index;
            }
        }

        int[] positions = new int[variableCount];
        for (int depth = 0; depth < orderVariables.length; depth++) {
            positions[orderVariables[depth]] = depth;
        }

        @SuppressWarnings("unchecked")
        List<Integer>[] activations = new List[variableCount];
        for (int i = 0; i < layout.relationCount(); i++) {
            Deadline.check(deadlineNanos);
            RelationBinding relation = layout.relation(i);
            int activationDepth = 0;
            for (int j = 0; j < relation.variableCount(); j++) {
                activationDepth = Math.max(activationDepth, positions[relation.variableIndex(j)]);
            }
            if (activations[activationDepth] == null) {
                activations[activationDepth] = new ArrayList<>();
            }
            activations[activationDepth].add(i);
        }

        int[][] activationGroups = new int[variableCount][];
        for (int depth = 0; depth < variableCount; depth++) {
            List<Integer> group = activations[depth];
            if (group == null || group.isEmpty()) {
                activationGroups[depth] = new int[0];
                continue;
            }
            int[] indexes = new int[group.size()];
            for (int i = 0; i < group.size(); i++) {
                indexes[i] = group.get(i);
            }
            activationGroups[depth] = indexes;
        }

        List<String> normalizedOrder = new ArrayList<>(variableCount);
        for (int variable : orderVariables) {
            normalizedOrder.add(layout.variableName(variable));
        }
        return new OrderContext(layout, List.copyOf(normalizedOrder), orderVariables, activationGroups);
    }

    private static void includeActivatedRelations(
            OrderContext context,
            OrderAccumulator accumulator,
            int depth,
            long deadlineNanos) {
        for (int relationIndex : context.activations(depth)) {
            Deadline.check(deadlineNanos);
            RelationBinding relation = context.relation(relationIndex);
            if (!accumulator.initialized()) {
                accumulator.initialize(SummaryState.from(relation, context.variableCount()));
            } else {
                accumulator.setSummary(joinSummary(accumulator.summary(), relation));
            }
        }
    }

    private static double estimateProjectedCardinality(
            OrderContext context,
            int[] projectedIndexes,
            OrderAccumulator accumulator,
            long deadlineNanos) {
        Deadline.check(deadlineNanos);
        if (!accumulator.initialized()) {
            return projectedIndexes.length == 0
                    ? boundaryPrefixEstimate(context, context.variableCount())
                    : boundaryPrefixEstimate(context, projectedIndexes);
        }
        return projectCardinality(accumulator.summary(), projectedIndexes);
    }

    private static int[] projectedIndexes(List<String> projected, RelationLayout layout) {
        if (projected.isEmpty()) {
            return new int[0];
        }
        int[] indexes = new int[projected.size()];
        int size = 0;
        for (String variable : projected) {
            Integer index = layout.variableIndex(variable);
            if (index != null) {
                indexes[size++] = index;
            }
        }
        if (size == indexes.length) {
            return indexes;
        }
        return java.util.Arrays.copyOf(indexes, size);
    }

    private static List<String> defaultOrder(Plan plan, List<Long> componentCounts) {
        Map<String, Long> minCountByVar = new HashMap<>();
        Map<String, Integer> participationByVar = new HashMap<>();
        List<Component> components = plan.components();
        for (int i = 0; i < components.size(); i++) {
            long count = componentCounts.isEmpty() ? 0L : componentCounts.get(i);
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
                        .comparingInt((String variable) -> participationByVar.getOrDefault(variable, 0))
                        .reversed()
                        .thenComparingLong(variable -> minCountByVar.getOrDefault(variable, Long.MAX_VALUE))
                        .thenComparing(Comparator.naturalOrder()))
                .toList();
    }

    private static List<String> normalizeOrder(List<String> variableOrder, List<RelationSummary> relations) {
        LinkedHashSet<String> variables = new LinkedHashSet<>(variableOrder);
        for (RelationSummary relation : relations) {
            variables.addAll(relation.variables());
        }
        return List.copyOf(variables);
    }

    private static int comparePlans(PlanSelection left, PlanSelection right) {
        int cmp = Double.compare(left.score(), right.score());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.plan().components().size(), right.plan().components().size());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.plan().maxDiameter(), right.plan().maxDiameter());
        if (cmp != 0) {
            return cmp;
        }
        return planSignature(left.plan()).compareTo(planSignature(right.plan()));
    }

    private static int compareOrders(OrderSelection left, OrderSelection right, List<String> projected) {
        int cmp = Double.compare(left.score(), right.score());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(projectedDepth(left.order(), projected), projectedDepth(right.order(), projected));
        if (cmp != 0) {
            return cmp;
        }
        return compareOrderLexicographically(left.order(), right.order());
    }

    private static int projectedDepth(List<String> order, List<String> projected) {
        if (projected.isEmpty()) {
            return order.size();
        }
        int depth = -1;
        for (String variable : projected) {
            int index = order.indexOf(variable);
            if (index > depth) {
                depth = index;
            }
        }
        return depth < 0 ? order.size() : depth;
    }

    private static int compareOrderLexicographically(List<String> left, List<String> right) {
        int size = Math.min(left.size(), right.size());
        for (int i = 0; i < size; i++) {
            int cmp = left.get(i).compareTo(right.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.size(), right.size());
    }

    private static String planSignature(Plan plan) {
        List<String> signatures = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            signatures.add(component.signature());
        }
        signatures.sort(String::compareTo);
        return String.join("|", signatures);
    }

    private static double normalizeCardinality(long value) {
        return normalizeCardinality((double) value);
    }

    private static double normalizeCardinality(double value) {
        if (!Double.isFinite(value) || value <= 0.0D) {
            return 0.0D;
        }
        return value;
    }

    private static double safeMultiply(double left, double right) {
        if (left <= 0.0D || right <= 0.0D) {
            return 0.0D;
        }
        if (!Double.isFinite(left) || !Double.isFinite(right)) {
            return Double.POSITIVE_INFINITY;
        }
        return normalizeCardinality(left * right);
    }

    private static double safeAdd(double left, double right) {
        if (!Double.isFinite(left) || !Double.isFinite(right)) {
            return Double.POSITIVE_INFINITY;
        }
        return normalizeCardinality(left + right);
    }

    public record PlanSelection(
            Plan plan,
            List<String> order,
            double score,
            double estimatedCount) {
    }

    public record OrderSelection(
            List<String> order,
            double score,
            double estimatedCount) {
    }

    private record StatsPlan(
            Plan plan,
            List<RelationSummary> relations,
            List<Long> componentCounts,
            boolean empty) {
    }

    private record RelationSummary(
            double cardinality,
            List<String> variables,
            Map<String, Double> distinctCounts,
            boolean empty) {
        private static RelationSummary from(Component component, RelationSynopsis synopsis) {
            Objects.requireNonNull(component, "component");
            Objects.requireNonNull(synopsis, "synopsis");
            if (synopsis.isEmpty()) {
                return new RelationSummary(0.0D, List.of(), Map.of(), true);
            }

            String left = component.sourceVarName();
            if (component.isUnary()) {
                return new RelationSummary(
                        normalizeCardinality(synopsis.tupleCount()),
                        List.of(left),
                        Map.of(left, normalizeCardinality(synopsis.distinctValues(Endpoint.SOURCE))),
                        false);
            }
            String right = component.targetVarName();
            return new RelationSummary(
                    normalizeCardinality(synopsis.tupleCount()),
                    List.of(left, right),
                    Map.of(
                            left, normalizeCardinality(synopsis.distinctValues(Endpoint.SOURCE)),
                            right, normalizeCardinality(synopsis.distinctValues(Endpoint.TARGET))),
                    false);
        }

        private static RelationSummary from(Relation relation) {
            Objects.requireNonNull(relation, "relation");
            List<String> variables = relation.variables();
            Map<String, Double> distinctCounts = new HashMap<>(variables.size());
            for (String variable : variables) {
                distinctCounts.put(variable, normalizeCardinality(relation.ndv(variable)));
            }
            return new RelationSummary(
                    normalizeCardinality(relation.tupleCount()),
                    List.copyOf(variables),
                    Map.copyOf(distinctCounts),
                    relation.tupleCount() <= 0L || variables.isEmpty());
        }

        private double distinctCount(String variable) {
            return distinctCounts.getOrDefault(variable, 0.0D);
        }
    }

    private record RelationLayout(
            List<String> variables,
            Map<String, Integer> variableIndexes,
            RelationBinding[] relations,
            double[] boundaryDistinctCounts,
            boolean[] presentInPlan) {
        private int variableCount() {
            return boundaryDistinctCounts.length;
        }

        private Integer variableIndex(String variable) {
            return variableIndexes.get(variable);
        }

        private String variableName(int index) {
            return variables.get(index);
        }

        private int relationCount() {
            return relations.length;
        }

        private RelationBinding relation(int index) {
            return relations[index];
        }

        private double boundaryDistinctCount(int variable) {
            return boundaryDistinctCounts[variable];
        }

        private boolean presentInPlan(int variable) {
            return presentInPlan[variable];
        }
    }

    private record OrderContext(
            RelationLayout layout,
            List<String> order,
            int[] orderVariables,
            int[][] activationGroups) {
        private int variableCount() {
            return orderVariables.length;
        }

        private RelationBinding relation(int index) {
            return layout.relation(index);
        }

        private int[] activations(int depth) {
            return activationGroups[depth];
        }

        private double boundaryDistinctCount(int variable) {
            return layout.boundaryDistinctCount(variable);
        }

        private boolean presentInPlan(int variable) {
            return layout.presentInPlan(variable);
        }

        private int variableAt(int depth) {
            return orderVariables[depth];
        }
    }

    private record RelationBinding(
            double cardinality,
            int[] variableIndexes,
            double[] distinctCounts) {
        private int variableCount() {
            return variableIndexes.length;
        }

        private int variableIndex(int index) {
            return variableIndexes[index];
        }

        private double distinctCount(int index) {
            return distinctCounts[index];
        }

        private int indexOf(int variable) {
            for (int i = 0; i < variableIndexes.length; i++) {
                if (variableIndexes[i] == variable) {
                    return i;
                }
            }
            return -1;
        }
    }

    private record SummaryState(
            double cardinality,
            boolean[] present,
            double[] distinctCounts) {
        private static SummaryState empty(int variableCount) {
            return new SummaryState(0.0D, new boolean[variableCount], new double[variableCount]);
        }

        private static SummaryState from(RelationBinding relation, int variableCount) {
            boolean[] present = new boolean[variableCount];
            double[] distinctCounts = new double[variableCount];
            for (int i = 0; i < relation.variableCount(); i++) {
                int variable = relation.variableIndex(i);
                present[variable] = true;
                distinctCounts[variable] = relation.distinctCount(i);
            }
            return new SummaryState(relation.cardinality(), present, distinctCounts);
        }

        private int variableCount() {
            return distinctCounts.length;
        }

        private boolean contains(int variable) {
            return present[variable];
        }

        private double distinctCount(int variable) {
            return distinctCounts[variable];
        }
    }

    private static final class OrderAccumulator {
        private final int variableCount;
        private SummaryState summary;
        private boolean initialized;

        private OrderAccumulator(int variableCount) {
            this.variableCount = variableCount;
        }

        private boolean initialized() {
            return initialized;
        }

        private SummaryState summary() {
            return summary;
        }

        private void initialize(SummaryState summary) {
            this.summary = summary;
            this.initialized = true;
        }

        private void setSummary(SummaryState summary) {
            this.summary = summary == null ? SummaryState.empty(variableCount) : summary;
        }
    }

}
