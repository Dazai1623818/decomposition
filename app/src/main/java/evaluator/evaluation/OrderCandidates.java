package evaluator.evaluation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Shared order-candidate generator for the simplified estimator-driven
 * variable-order search.
 * <p>
 * The estimator no longer performs projected-first or exact permutation
 * search. It only compares a small deterministic neighborhood around the
 * supplied heuristic base order.
 */
final class OrderCandidates {
    private OrderCandidates() {
    }

    static List<List<String>> build(List<String> baseOrder, List<String> projected) {
        return buildLocal(baseOrder, projected, 0);
    }

    static List<List<String>> build(List<String> baseOrder, List<String> projected, int maxCandidateOrders) {
        return buildLocal(baseOrder, projected, maxCandidateOrders);
    }

    static List<List<String>> buildLocal(List<String> baseOrder, List<String> projected, int maxCandidateOrders) {
        List<String> base = List.copyOf(baseOrder);
        LinkedHashSet<List<String>> candidates = new LinkedHashSet<>();
        candidates.add(base);
        addSingleInsertionMoves(candidates, base);
        List<List<String>> ranked = rankLocalCandidates(List.copyOf(candidates), base);
        if (maxCandidateOrders > 0 && ranked.size() > maxCandidateOrders) {
            return List.copyOf(ranked.subList(0, maxCandidateOrders));
        }
        return ranked;
    }

    private static void addSingleInsertionMoves(
            LinkedHashSet<List<String>> candidates,
            List<String> baseOrder) {
        for (int from = 0; from < baseOrder.size(); from++) {
            for (int to = 0; to < baseOrder.size(); to++) {
                if (from == to) {
                    continue;
                }
                candidates.add(moved(baseOrder, from, to));
            }
        }
    }

    private static List<List<String>> rankLocalCandidates(
            List<List<String>> candidates,
            List<String> baseOrder) {
        List<List<String>> ranked = new ArrayList<>(candidates);
        ranked.sort(Comparator
                .comparingInt((List<String> order) -> orderDistance(order, baseOrder))
                .thenComparing(OrderCandidates::compareLexicographically));
        return List.copyOf(ranked);
    }

    private static List<String> moved(List<String> order, int from, int to) {
        List<String> reordered = new ArrayList<>(order);
        String variable = reordered.remove(from);
        reordered.add(to, variable);
        return List.copyOf(reordered);
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
}
