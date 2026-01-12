package evaluator.join;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class LeapfrogTrieJoin {
    private LeapfrogTrieJoin() {
    }

    public static List<Map<String, Integer>> join(List<RelationBinding> relations, List<String> variableOrder) {
        Objects.requireNonNull(relations, "relations");
        if (relations.isEmpty()) {
            return List.of();
        }

        Map<String, List<RelationBinding>> bindingsByVar = new HashMap<>();
        for (RelationBinding binding : relations) {
            binding.register(bindingsByVar);
        }

        List<String> order = (variableOrder == null || variableOrder.isEmpty())
                ? bindingsByVar.keySet().stream()
                .sorted(
                        Comparator.comparingInt((String var) -> bindingsByVar.get(var).size())
                                .reversed()
                                .thenComparing(Comparator.naturalOrder()))
                .toList()
                : variableOrder;

        List<Map<String, Integer>> results = new ArrayList<>();
        search(order, 0, bindingsByVar, new LinkedHashMap<>(), results);
        return results;
    }

    private static void search(
            List<String> order,
            int depth,
            Map<String, List<RelationBinding>> bindings,
            Map<String, Integer> assignment,
            List<Map<String, Integer>> results) {

        if (depth == order.size()) {
            results.add(new LinkedHashMap<>(assignment));
            return;
        }

        String variable = order.get(depth);
        List<RelationBinding> constraints = bindings.getOrDefault(variable, List.of());
        if (constraints.isEmpty()) {
            return;
        }

        List<IntCursor> cursors = new ArrayList<>(constraints.size());
        for (RelationBinding binding : constraints) {
            int[] domain = binding.domainFor(variable, assignment);
            if (domain.length == 0) {
                return;
            }
            cursors.add(new IntCursor(domain));
        }

        LeapfrogIterator iterator = new LeapfrogIterator(cursors);
        iterator.init();
        while (!iterator.atEnd()) {
            assignment.put(variable, iterator.key());
            search(order, depth + 1, bindings, assignment, results);
            assignment.remove(variable);
            iterator.next();
        }
    }

    private static final class LeapfrogIterator {
        private final List<IntCursor> cursors;
        private int p = 0;
        private boolean atEnd = false;

        LeapfrogIterator(List<IntCursor> cursors) {
            this.cursors = Objects.requireNonNull(cursors, "cursors");
            if (cursors.isEmpty()) {
                throw new IllegalArgumentException("cursors must not be empty");
            }
        }

        void init() {
            cursors.sort(Comparator.comparingInt(IntCursor::key));
            p = 0;
            leapfrogSearch();
        }

        boolean atEnd() {
            return atEnd;
        }

        int key() {
            if (atEnd) {
                throw new IllegalStateException("atEnd");
            }
            return cursors.get(p).key();
        }

        void next() {
            if (atEnd) {
                return;
            }
            cursors.get(p).next();
            p = (p + 1) % cursors.size();
            leapfrogSearch();
        }

        private void leapfrogSearch() {
            while (true) {
                IntCursor max = maxCursor();
                int maxKey = max.key();

                IntCursor cur = cursors.get(p);
                cur.seek(maxKey);
                if (cur.atEnd()) {
                    atEnd = true;
                    return;
                }

                if (cur.key() == maxKey) {
                    IntCursor min = minCursor();
                    if (min.key() == maxKey) {
                        return;
                    }
                    p = cursors.indexOf(min);
                } else {
                    p = (p + 1) % cursors.size();
                }
            }
        }

        private IntCursor maxCursor() {
            IntCursor max = cursors.get(0);
            for (int i = 1; i < cursors.size(); i++) {
                IntCursor c = cursors.get(i);
                if (c.key() > max.key()) {
                    max = c;
                }
            }
            return max;
        }

        private IntCursor minCursor() {
            IntCursor min = cursors.get(0);
            for (int i = 1; i < cursors.size(); i++) {
                IntCursor c = cursors.get(i);
                if (c.key() < min.key()) {
                    min = c;
                }
            }
            return min;
        }
    }

    private static final class IntCursor {
        private final int[] data;
        private int pos;

        IntCursor(int[] data) {
            this.data = Objects.requireNonNull(data, "data");
            this.pos = 0;
        }

        boolean atEnd() {
            return pos >= data.length;
        }

        int key() {
            if (atEnd()) {
                return Integer.MAX_VALUE;
            }
            return data[pos];
        }

        void next() {
            pos++;
        }

        void seek(int target) {
            while (!atEnd() && key() < target) {
                pos++;
            }
        }
    }
}
