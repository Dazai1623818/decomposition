package evaluator.evaluation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared order-candidate generator for estimator-driven variable-order search.
 * Small queries are explored exactly, while larger queries use a compact
 * heuristic neighborhood that keeps projected variables early.
 */
final class OrderCandidates {
    private static final int EXACT_ORDER_VARIABLE_LIMIT = 6;
    private static final int LARGE_QUERY_MOVE_LIMIT = 3;

    private OrderCandidates() {
    }

    static List<List<String>> build(List<String> baseOrder, List<String> projected) {
        return build(baseOrder, projected, 0);
    }

    static List<List<String>> build(List<String> baseOrder, List<String> projected, int maxCandidateOrders) {
        List<String> base = List.copyOf(baseOrder);
        List<String> projectedPrefix = projectedPrefix(base, projected);
        List<List<String>> candidates;
        if (base.size() <= EXACT_ORDER_VARIABLE_LIMIT) {
            candidates = exactOrders(base, projectedPrefix);
        } else {
            candidates = heuristicOrders(base, projectedPrefix);
        }
        List<List<String>> ranked = rankCandidates(candidates, base, projectedPrefix);
        return applyBudget(ranked, maxCandidateOrders);
    }

    private static List<List<String>> exactOrders(List<String> baseOrder, List<String> projectedPrefix) {
        if (projectedPrefix.isEmpty() || projectedPrefix.size() == baseOrder.size()) {
            return permutations(baseOrder);
        }

        Set<String> projectedSet = new HashSet<>(projectedPrefix);
        List<String> remainder = new ArrayList<>(baseOrder.size() - projectedPrefix.size());
        for (String variable : baseOrder) {
            if (!projectedSet.contains(variable)) {
                remainder.add(variable);
            }
        }

        List<List<String>> candidates = new ArrayList<>();
        List<List<String>> projectedOrders = permutations(projectedPrefix);
        List<List<String>> remainderOrders = permutations(remainder);
        for (List<String> projectedOrder : projectedOrders) {
            for (List<String> remainderOrder : remainderOrders) {
                List<String> combined = new ArrayList<>(baseOrder.size());
                combined.addAll(projectedOrder);
                combined.addAll(remainderOrder);
                candidates.add(List.copyOf(combined));
            }
        }
        return List.copyOf(candidates);
    }

    private static List<List<String>> heuristicOrders(List<String> baseOrder, List<String> projectedPrefix) {
        LinkedHashSet<List<String>> candidates = new LinkedHashSet<>();
        List<String> projectedFirst = projectedFirst(baseOrder, projectedPrefix);
        candidates.add(baseOrder);
        candidates.add(projectedFirst);

        addMoveFrontOrders(candidates, baseOrder, LARGE_QUERY_MOVE_LIMIT);
        addMoveFrontOrders(candidates, projectedFirst, LARGE_QUERY_MOVE_LIMIT);
        if (!projectedPrefix.isEmpty()) {
            addMoveFrontOrders(candidates, projectedFirst, projectedPrefix, LARGE_QUERY_MOVE_LIMIT);
        }

        return List.copyOf(candidates);
    }

    private static List<List<String>> rankCandidates(
            List<List<String>> candidates,
            List<String> baseOrder,
            List<String> projectedPrefix) {
        List<String> projectedFirst = projectedFirst(baseOrder, projectedPrefix);
        List<List<String>> ranked = new ArrayList<>(candidates);
        ranked.sort(Comparator
                .comparingInt((List<String> order) -> projectedDepth(order, projectedPrefix))
                .thenComparingInt(order -> orderDistance(order, projectedFirst))
                .thenComparingInt(order -> orderDistance(order, baseOrder))
                .thenComparing(OrderCandidates::compareLexicographically));
        return List.copyOf(ranked);
    }

    private static List<List<String>> applyBudget(List<List<String>> ranked, int maxCandidateOrders) {
        if (maxCandidateOrders <= 0 || ranked.size() <= maxCandidateOrders) {
            return ranked;
        }
        return List.copyOf(ranked.subList(0, maxCandidateOrders));
    }

    private static void addMoveFrontOrders(
            LinkedHashSet<List<String>> candidates,
            List<String> order,
            int limit) {
        for (int i = 0; i < Math.min(limit, order.size()); i++) {
            candidates.add(moveToFront(order, order.get(i)));
        }
    }

    private static void addMoveFrontOrders(
            LinkedHashSet<List<String>> candidates,
            List<String> order,
            List<String> anchors,
            int limit) {
        int added = 0;
        for (String anchor : anchors) {
            candidates.add(moveToFront(order, anchor));
            added++;
            if (added >= limit) {
                break;
            }
        }
    }

    private static List<String> projectedPrefix(List<String> baseOrder, List<String> projected) {
        if (projected.isEmpty()) {
            return List.of();
        }

        Set<String> projectedSet = new HashSet<>(projected);
        List<String> prefix = new ArrayList<>(projected.size());
        for (String variable : baseOrder) {
            if (projectedSet.contains(variable)) {
                prefix.add(variable);
            }
        }
        return List.copyOf(prefix);
    }

    private static List<String> projectedFirst(List<String> order, List<String> projectedPrefix) {
        if (projectedPrefix.isEmpty()) {
            return order;
        }

        Set<String> projectedSet = new HashSet<>(projectedPrefix);
        List<String> reordered = new ArrayList<>(order.size());
        reordered.addAll(projectedPrefix);
        for (String variable : order) {
            if (!projectedSet.contains(variable)) {
                reordered.add(variable);
            }
        }
        return List.copyOf(reordered);
    }

    private static List<String> moveToFront(List<String> order, String variable) {
        if (order.isEmpty() || variable.equals(order.get(0))) {
            return order;
        }
        List<String> reordered = new ArrayList<>(order.size());
        reordered.add(variable);
        for (String candidate : order) {
            if (!candidate.equals(variable)) {
                reordered.add(candidate);
            }
        }
        return List.copyOf(reordered);
    }

    private static int projectedDepth(List<String> order, List<String> projectedPrefix) {
        if (projectedPrefix.isEmpty()) {
            return order.size();
        }
        int depth = -1;
        for (String variable : projectedPrefix) {
            int index = order.indexOf(variable);
            if (index > depth) {
                depth = index;
            }
        }
        return depth < 0 ? order.size() : depth;
    }

    private static int orderDistance(List<String> order, List<String> reference) {
        int distance = 0;
        for (int i = 0; i < order.size(); i++) {
            distance += Math.abs(i - reference.indexOf(order.get(i)));
        }
        return distance;
    }

    private static int compareLexicographically(List<String> left, List<String> right) {
        int common = Math.min(left.size(), right.size());
        for (int i = 0; i < common; i++) {
            int cmp = left.get(i).compareTo(right.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.size(), right.size());
    }

    private static List<List<String>> permutations(List<String> values) {
        if (values.isEmpty()) {
            return List.of(List.of());
        }
        List<List<String>> permutations = new ArrayList<>();
        permute(new ArrayList<>(values), 0, permutations);
        return List.copyOf(permutations);
    }

    private static void permute(List<String> values, int index, List<List<String>> output) {
        if (index == values.size()) {
            output.add(List.copyOf(values));
            return;
        }
        for (int i = index; i < values.size(); i++) {
            swap(values, index, i);
            permute(values, index + 1, output);
            swap(values, index, i);
        }
    }

    private static void swap(List<String> values, int left, int right) {
        if (left == right) {
            return;
        }
        String tmp = values.get(left);
        values.set(left, values.get(right));
        values.set(right, tmp);
    }
}
