package evaluator.evaluation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Wander Join estimator for projected join cardinality.
 */
public final class WanderJoinEstimator {
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private WanderJoinEstimator() {
    }

    public record Estimate(double estimatedCount, double standardError) {
    }

    /**
     * Per-prefix projected-cardinality estimate for variable-order prefixes.
     *
     * @param estimatedCount Estimated projected cardinality for prefix 1..step.
     * @param standardError  Standard error of the estimate.
     * @param estimateNanos  Time spent computing this prefix estimate.
     */
    public record PrefixEstimate(double estimatedCount, double standardError, long estimateNanos) {
    }

    public static Estimate estimateProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projectedVars,
            int walks,
            long seed) {
        return estimateProjectedCount(relations, variableOrder, projectedVars, walks, seed, true);
    }

    public static Estimate estimateProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projectedVars,
            int walks,
            long seed,
            boolean requireExtensionCheck) {
        Objects.requireNonNull(relations, "relations");
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(projectedVars, "projectedVars");
        if (walks < 1) {
            throw new IllegalArgumentException("walks must be >= 1");
        }
        if (relations.isEmpty() || projectedVars.isEmpty()) {
            return new Estimate(0.0, 0.0);
        }

        Map<String, List<Relation>> bindingsByVar = buildBindingsByVar(relations);
        Map<String, Integer> indexByVar = buildIndexByVar(variableOrder);
        List<String> projectedOrder = buildProjectedOrder(variableOrder, projectedVars, indexByVar);
        if (projectedOrder.isEmpty()) {
            return new Estimate(0.0, 0.0);
        }

        Random random = new Random(seed);
        int[] assignment = new int[variableOrder.size()];
        boolean[] bound = new boolean[variableOrder.size()];
        int[] touched = new int[projectedOrder.size()];

        double sum = 0.0;
        double sumSquares = 0.0;

        for (int walk = 0; walk < walks; walk++) {
            checkInterrupted();
            double weight = 1.0;
            boolean alive = true;
            int touchedCount = 0;

            for (String variable : projectedOrder) {
                checkInterrupted();
                List<Relation> constraints = bindingsByVar.getOrDefault(variable, List.of());
                if (constraints.isEmpty()) {
                    alive = false;
                    break;
                }
                int[] domain = intersectDomains(constraints, variable, assignment, bound, indexByVar);
                if (domain.length == 0) {
                    alive = false;
                    break;
                }
                weight *= domain.length;

                int variableIndex = indexByVar.get(variable);
                int value = domain[random.nextInt(domain.length)];
                assignment[variableIndex] = value;
                bound[variableIndex] = true;
                touched[touchedCount++] = variableIndex;
            }

            if (alive && requireExtensionCheck && !existsExtension(variableOrder, 0, bindingsByVar, assignment, bound, indexByVar)) {
                alive = false;
            }

            double sample = alive ? weight : 0.0;
            sum += sample;
            sumSquares += sample * sample;

            for (int i = 0; i < touchedCount; i++) {
                bound[touched[i]] = false;
            }
        }

        double mean = sum / walks;
        if (walks == 1) {
            return new Estimate(mean, 0.0);
        }

        double variance = (sumSquares - (sum * sum) / walks) / (walks - 1);
        if (variance < 0.0) {
            variance = 0.0;
        }
        return new Estimate(mean, Math.sqrt(variance / walks));
    }

    /**
     * Estimates projected cardinalities for all prefixes of {@code variableOrder}
     * using a single absolute walk budget. Prefix {@code i} corresponds to
     * projected variables {@code variableOrder[0..i]}.
     *
     * @param relations     Join relations.
     * @param variableOrder Variable order used by the join.
     * @param walks         Absolute number of random trials.
     * @param seed          Random seed for reproducibility.
     * @return Prefix estimates in order of increasing prefix length.
     */
    public static List<PrefixEstimate> estimateProjectedCountPrefixes(
            List<Relation> relations,
            List<String> variableOrder,
            int walks,
            long seed) {
        return estimateProjectedCountPrefixes(relations, variableOrder, walks, seed, true);
    }

    public static List<PrefixEstimate> estimateProjectedCountPrefixes(
            List<Relation> relations,
            List<String> variableOrder,
            int walks,
            long seed,
            boolean requireExtensionCheck) {
        Objects.requireNonNull(relations, "relations");
        Objects.requireNonNull(variableOrder, "variableOrder");
        if (walks < 1) {
            throw new IllegalArgumentException("walks must be >= 1");
        }
        if (variableOrder.isEmpty()) {
            return List.of();
        }
        if (relations.isEmpty()) {
            return zeroPrefixEstimates(variableOrder.size());
        }

        Map<String, List<Relation>> bindingsByVar = buildBindingsByVar(relations);
        Map<String, Integer> indexByVar = buildIndexByVar(variableOrder);
        Random random = new Random(seed);
        int[] assignment = new int[variableOrder.size()];
        boolean[] bound = new boolean[variableOrder.size()];
        int[] touched = new int[variableOrder.size()];

        double[] sum = new double[variableOrder.size()];
        double[] sumSquares = new double[variableOrder.size()];
        long[] estimateNanos = new long[variableOrder.size()];

        for (int walk = 0; walk < walks; walk++) {
            checkInterrupted();
            double weight = 1.0D;
            int touchedCount = 0;

            for (int depth = 0; depth < variableOrder.size(); depth++) {
                long stepStart = System.nanoTime();
                checkInterrupted();

                String variable = variableOrder.get(depth);
                List<Relation> constraints = bindingsByVar.getOrDefault(variable, List.of());
                if (constraints.isEmpty()) {
                    estimateNanos[depth] += System.nanoTime() - stepStart;
                    break;
                }

                int[] domain = intersectDomains(constraints, variable, assignment, bound, indexByVar);
                if (domain.length == 0) {
                    estimateNanos[depth] += System.nanoTime() - stepStart;
                    break;
                }

                weight *= domain.length;
                int value = domain[random.nextInt(domain.length)];
                int variableIndex = indexByVar.get(variable);
                assignment[variableIndex] = value;
                bound[variableIndex] = true;
                touched[touchedCount++] = variableIndex;

                boolean alive = !requireExtensionCheck
                        || existsExtension(variableOrder, depth + 1, bindingsByVar, assignment, bound, indexByVar);
                double sample = alive ? weight : 0.0D;
                sum[depth] += sample;
                sumSquares[depth] += sample * sample;
                estimateNanos[depth] += System.nanoTime() - stepStart;
                if (!alive) {
                    break;
                }
            }

            for (int i = 0; i < touchedCount; i++) {
                bound[touched[i]] = false;
            }
        }

        List<PrefixEstimate> estimates = new ArrayList<>(variableOrder.size());
        for (int depth = 0; depth < variableOrder.size(); depth++) {
            double mean = sum[depth] / walks;
            if (walks == 1) {
                estimates.add(new PrefixEstimate(mean, 0.0D, estimateNanos[depth]));
                continue;
            }
            double variance = (sumSquares[depth] - (sum[depth] * sum[depth]) / walks) / (walks - 1);
            if (variance < 0.0D) {
                variance = 0.0D;
            }
            estimates.add(new PrefixEstimate(mean, Math.sqrt(variance / walks), estimateNanos[depth]));
        }
        return List.copyOf(estimates);
    }

    private static List<PrefixEstimate> zeroPrefixEstimates(int size) {
        List<PrefixEstimate> estimates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            estimates.add(new PrefixEstimate(0.0D, 0.0D, 0L));
        }
        return List.copyOf(estimates);
    }

    private static Map<String, List<Relation>> buildBindingsByVar(List<Relation> relations) {
        Map<String, List<Relation>> bindingsByVar = new HashMap<>();
        for (Relation binding : relations) {
            binding.register(bindingsByVar);
        }
        return bindingsByVar;
    }

    private static Map<String, Integer> buildIndexByVar(List<String> order) {
        Map<String, Integer> indexByVar = new HashMap<>(order.size());
        for (int i = 0; i < order.size(); i++) {
            String variable = order.get(i);
            if (indexByVar.put(variable, i) != null) {
                throw new IllegalArgumentException("Duplicate variable in order: " + variable);
            }
        }
        return indexByVar;
    }

    private static List<String> buildProjectedOrder(
            List<String> variableOrder,
            List<String> projectedVars,
            Map<String, Integer> indexByVar) {
        Set<String> projected = new HashSet<>();
        for (String variable : projectedVars) {
            if (!projected.add(variable)) {
                throw new IllegalArgumentException("Duplicate projected variable " + variable);
            }
            if (!indexByVar.containsKey(variable)) {
                throw new IllegalArgumentException("Unknown projected variable " + variable);
            }
        }

        List<String> order = new ArrayList<>(projected.size());
        for (String variable : variableOrder) {
            if (projected.contains(variable)) {
                order.add(variable);
            }
        }
        return order;
    }

    private static boolean existsExtension(
            List<String> variableOrder,
            int depth,
            Map<String, List<Relation>> bindingsByVar,
            int[] assignment,
            boolean[] bound,
            Map<String, Integer> indexByVar) {
        checkInterrupted();
        if (depth == variableOrder.size()) {
            return true;
        }

        String variable = variableOrder.get(depth);
        int variableIndex = depth;
        List<Relation> constraints = bindingsByVar.getOrDefault(variable, List.of());
        if (constraints.isEmpty()) {
            return false;
        }

        if (bound[variableIndex]) {
            if (!valueAllowed(variable, assignment[variableIndex], constraints, assignment, bound, indexByVar)) {
                return false;
            }
            return existsExtension(variableOrder, depth + 1, bindingsByVar, assignment, bound, indexByVar);
        }

        int[] domain = intersectDomains(constraints, variable, assignment, bound, indexByVar);
        for (int value : domain) {
            checkInterrupted();
            assignment[variableIndex] = value;
            bound[variableIndex] = true;
            if (existsExtension(variableOrder, depth + 1, bindingsByVar, assignment, bound, indexByVar)) {
                bound[variableIndex] = false;
                return true;
            }
            bound[variableIndex] = false;
        }

        return false;
    }

    private static boolean valueAllowed(
            String variable,
            int value,
            List<Relation> constraints,
            int[] assignment,
            boolean[] bound,
            Map<String, Integer> indexByVar) {
        for (Relation binding : constraints) {
            int[] domain = binding.domainFor(variable, assignment, bound, indexByVar);
            if (domain.length == 0) {
                return false;
            }
            if (Arrays.binarySearch(domain, value) < 0) {
                return false;
            }
        }
        return true;
    }

    private static int[] intersectDomains(
            List<Relation> constraints,
            String variable,
            int[] assignment,
            boolean[] bound,
            Map<String, Integer> indexByVar) {
        int[] intersection = null;
        for (Relation binding : constraints) {
            checkInterrupted();
            int[] domain = binding.domainFor(variable, assignment, bound, indexByVar);
            if (domain.length == 0) {
                return EMPTY_INT_ARRAY;
            }
            if (intersection == null) {
                intersection = domain;
            } else {
                intersection = intersectSortedDistinct(intersection, domain);
            }
            if (intersection.length == 0) {
                return EMPTY_INT_ARRAY;
            }
        }
        return intersection == null ? EMPTY_INT_ARRAY : intersection;
    }

    private static int[] intersectSortedDistinct(int[] left, int[] right) {
        int[] out = new int[Math.min(left.length, right.length)];
        int i = 0;
        int j = 0;
        int size = 0;
        while (i < left.length && j < right.length) {
            int lv = left[i];
            int rv = right[j];
            if (lv == rv) {
                out[size++] = lv;
                i++;
                j++;
            } else if (lv < rv) {
                i++;
            } else {
                j++;
            }
        }
        if (size == 0) {
            return EMPTY_INT_ARRAY;
        }
        return size == out.length ? out : Arrays.copyOf(out, size);
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new EstimationInterruptedException();
        }
    }

    private static final class EstimationInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
