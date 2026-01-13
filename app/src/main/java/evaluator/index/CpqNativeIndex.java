package evaluator.index;

import evaluator.cq.ConjunctiveQuery;
import evaluator.decompose.CpqDecomposition;
import evaluator.decompose.CpqDecomposition.Component;
import evaluator.join.LeapfrogTrieJoin;
import evaluator.join.RelationBinding;
import evaluator.join.RelationBinding.RelationProjection;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class CpqNativeIndex {
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    static {
        try {
            Main.loadNatives();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Index index;
    private final int k;

    private CpqNativeIndex(Index index) {
        this.index = Objects.requireNonNull(index, "index");
        this.k = 2;
    }

    public static CpqNativeIndex load(Path savedIndexFile) throws Exception {
        Objects.requireNonNull(savedIndexFile, "savedIndexFile");
        try (InputStream in = Files.newInputStream(savedIndexFile)) {
            Index index = new Index(in);
            return new CpqNativeIndex(index);
        }
    }

    public int k() {
        return k;
    }

    public ConjunctiveQuery parseCQ(String text) {
        return ConjunctiveQuery.parse(text);
    }

    public List<Map<String, Integer>> evaluate(CpqDecomposition decomposition) {
        Objects.requireNonNull(decomposition, "decomposition");

        List<Component> components = decomposition.components();
        if (components.isEmpty()) {
            return List.of();
        }

        List<RelationBinding> relations = components.stream()
                .map(this::evaluateComponent)
                .filter(Objects::nonNull)
                .toList();

        if (relations.size() != components.size()) {
            return List.of();
        }

        List<Map<String, Integer>> raw = LeapfrogTrieJoin.join(relations, decomposition.variableOrder());
        return project(raw, decomposition.freeVars().stream().map(CpqDecomposition::varName).toList());
    }

    private RelationBinding evaluateComponent(Component component) {
        List<Pair> matches = index.query(component.cpq());

        String left = CpqDecomposition.varName(component.s());
        String right = CpqDecomposition.varName(component.t());
        String description = component.canonical();

        if (left.equals(right)) {
            Set<Integer> values = new HashSet<>();
            for (Pair pair : matches) {
                if (pair.getSource() == pair.getTarget()) {
                    values.add(pair.getSource());
                }
            }
            if (values.isEmpty()) {
                return null;
            }
            int[] domain = values.stream().mapToInt(Integer::intValue).sorted().toArray();
            return RelationBinding.unary(left, description, domain);
        }

        Map<Integer, IntAccumulator> forward = new HashMap<>();
        Map<Integer, IntAccumulator> reverse = new HashMap<>();
        for (Pair pair : matches) {
            forward.computeIfAbsent(pair.getSource(), ignored -> new IntAccumulator()).add(pair.getTarget());
            reverse.computeIfAbsent(pair.getTarget(), ignored -> new IntAccumulator()).add(pair.getSource());
        }

        if (forward.isEmpty() || reverse.isEmpty()) {
            return null;
        }

        RelationProjection projection = new RelationProjection(
                sortedKeys(forward.keySet()),
                sortedKeys(reverse.keySet()),
                toIntArrayMap(forward),
                toIntArrayMap(reverse));

        if (projection.isEmpty()) {
            return null;
        }
        return RelationBinding.binary(left, right, description, projection);
    }

    private static int[] sortedKeys(Set<Integer> keys) {
        if (keys.isEmpty()) {
            return EMPTY_INT_ARRAY;
        }
        int[] sorted = keys.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(sorted);
        return sorted;
    }

    private static Map<Integer, int[]> toIntArrayMap(Map<Integer, IntAccumulator> input) {
        Map<Integer, int[]> result = new HashMap<>(input.size());
        for (Map.Entry<Integer, IntAccumulator> entry : input.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toSortedDistinctArray());
        }
        return result;
    }

    private static List<Map<String, Integer>> project(List<Map<String, Integer>> rows, List<String> vars) {
        if (rows.isEmpty()) {
            return rows;
        }
        if (vars.isEmpty()) {
            return List.of(Map.of());
        }

        Set<List<Integer>> seen = new HashSet<>();
        List<Map<String, Integer>> out = new java.util.ArrayList<>(rows.size());
        for (Map<String, Integer> row : rows) {
            List<Integer> key = vars.stream().map(row::get).toList();
            if (seen.add(key)) {
                Map<String, Integer> projected = new java.util.LinkedHashMap<>(vars.size());
                for (String v : vars) {
                    projected.put(v, row.get(v));
                }
                out.add(projected);
            }
        }
        return out;
    }

    private static final class IntAccumulator {
        private int[] data = new int[8];
        private int size = 0;

        void add(int value) {
            if (size == data.length) {
                data = Arrays.copyOf(data, Math.max(8, data.length * 2));
            }
            data[size++] = value;
        }

        int[] toSortedDistinctArray() {
            if (size == 0) {
                return EMPTY_INT_ARRAY;
            }
            int[] out = Arrays.copyOf(data, size);
            Arrays.sort(out);
            int unique = 1;
            for (int i = 1; i < out.length; i++) {
                if (out[i] != out[unique - 1]) {
                    out[unique++] = out[i];
                }
            }
            return unique == out.length ? out : Arrays.copyOf(out, unique);
        }
    }
}
