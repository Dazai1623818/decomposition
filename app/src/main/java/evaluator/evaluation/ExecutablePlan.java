package evaluator.evaluation;

import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Execution-time representation of a compiled plan.
 */
public final class ExecutablePlan {
    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final int[][] EMPTY_INT_MATRIX = new int[0][];

    private final Plan plan;
    private final List<Relation> relations;
    private final List<Long> componentCounts;
    private final CompilationStats compilationStats;
    private final boolean empty;

    public ExecutablePlan(
            Plan plan,
            List<Relation> relations,
            List<Long> componentCounts,
            CompilationStats compilationStats,
            boolean empty) {
        this.plan = Objects.requireNonNull(plan, "plan");
        this.relations = List.copyOf(Objects.requireNonNull(relations, "relations"));
        this.componentCounts = List.copyOf(Objects.requireNonNull(componentCounts, "componentCounts"));
        this.compilationStats = Objects.requireNonNull(compilationStats, "compilationStats");
        this.empty = empty;
    }

    public Plan plan() {
        return plan;
    }

    public boolean isEmpty() {
        return empty;
    }

    public List<Long> componentCounts() {
        return componentCounts;
    }

    public List<Component> components() {
        return plan.components();
    }

    public List<Relation> relations() {
        return relations;
    }

    public CompilationStats compilationStats() {
        return compilationStats;
    }

    public static ExecutablePlan compile(Plan plan, CpqIndex index) {
        return compile(plan, index, Long.MAX_VALUE);
    }

    public static ExecutablePlan compile(Plan plan, CpqIndex index, long deadlineNanos) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(index, "index");

        List<Component> components = plan.components();
        long queryNanos = 0L;
        long mappingNanos = 0L;
        if (components.isEmpty()) {
            return new ExecutablePlan(
                    plan,
                    List.of(),
                    List.of(),
                    new CompilationStats(queryNanos, mappingNanos),
                    true);
        }

        List<Relation> relations = new ArrayList<>(components.size());
        List<Long> componentCounts = new ArrayList<>(components.size());
        for (Component component : components) {
            Deadline.check(deadlineNanos);
            long queryStart = System.nanoTime();
            List<CpqIndex.Edge> matches = index.query(component.cpq());
            queryNanos += System.nanoTime() - queryStart;

            long mappingStart = System.nanoTime();
            CompiledComponent compiled;
            try {
                compiled = evaluateComponent(component, matches, deadlineNanos);
            } finally {
                mappingNanos += System.nanoTime() - mappingStart;
            }

            if (compiled == null) {
                return new ExecutablePlan(
                        plan,
                        List.of(),
                        List.of(),
                        new CompilationStats(queryNanos, mappingNanos),
                        true);
            }
            relations.add(compiled.binding());
            componentCounts.add(compiled.count());
        }

        if (relations.size() != components.size()) {
            return new ExecutablePlan(
                    plan,
                    List.of(),
                    List.of(),
                    new CompilationStats(queryNanos, mappingNanos),
                    true);
        }

        return new ExecutablePlan(
                plan,
                relations,
                componentCounts,
                new CompilationStats(queryNanos, mappingNanos),
                false);
    }

    public LeapfrogJoin.JoinResult join(
            List<String> variableOrder,
            LeapfrogJoin.JoinMode mode,
            boolean safeDistinctFastPath) {
        return join(variableOrder, mode, safeDistinctFastPath, Long.MAX_VALUE);
    }

    public LeapfrogJoin.JoinResult join(
            List<String> variableOrder,
            LeapfrogJoin.JoinMode mode,
            boolean safeDistinctFastPath,
            long deadlineNanos) {
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(mode, "mode");
        return LeapfrogJoin.join(
                relations,
                variableOrder,
                plan.projectedVariableNames(),
                mode,
                safeDistinctFastPath,
                deadlineNanos);
    }

    public WanderJoinEstimator.Estimate estimateProjectedCount(
            List<String> variableOrder,
            List<String> projectedVars,
            int walks,
            long seed) {
        return estimateProjectedCount(variableOrder, projectedVars, walks, seed, Long.MAX_VALUE);
    }

    public WanderJoinEstimator.Estimate estimateProjectedCount(
            List<String> variableOrder,
            List<String> projectedVars,
            int walks,
            long seed,
            long deadlineNanos) {
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(projectedVars, "projectedVars");
        if (empty) {
            return new WanderJoinEstimator.Estimate(0.0, 0.0);
        }
        return WanderJoinEstimator.estimateProjectedCount(
                relations,
                variableOrder,
                projectedVars,
                walks,
                seed,
                deadlineNanos);
    }

    private static CompiledComponent evaluateComponent(
            Component component,
            List<CpqIndex.Edge> matches,
            long deadlineNanos) {
        String left = component.sourceVarName();
        String right = component.targetVarName();
        String description = component.normalized();
        long resultCount = matches.size();
        if (left.equals(right)) {
            return buildUnaryBinding(left, description, matches, resultCount, deadlineNanos);
        }
        return buildBinaryBinding(left, right, description, matches, resultCount, deadlineNanos);
    }

    private static CompiledComponent buildUnaryBinding(
            String variable,
            String description,
            List<CpqIndex.Edge> matches,
            long resultCount,
            long deadlineNanos) {
        IntAccumulator values = new IntAccumulator(matches.size());
        for (CpqIndex.Edge pair : matches) {
            Deadline.check(deadlineNanos);
            values.add(pair.source());
        }
        int[] domain = values.toSortedDistinctArray();
        if (domain.length == 0) {
            return null;
        }
        return new CompiledComponent(Relation.unary(variable, description, domain), resultCount);
    }

    private static CompiledComponent buildBinaryBinding(
            String left,
            String right,
            String description,
            List<CpqIndex.Edge> matches,
            long resultCount,
            long deadlineNanos) {
        IntAccumulatorMap forward = new IntAccumulatorMap();
        IntAccumulatorMap reverse = new IntAccumulatorMap();
        for (CpqIndex.Edge pair : matches) {
            Deadline.check(deadlineNanos);
            forward.add(pair.source(), pair.target());
            reverse.add(pair.target(), pair.source());
        }

        if (forward.isEmpty() || reverse.isEmpty()) {
            return null;
        }

        IntArrayLookup forwardLookup = forward.toDenseLookup();
        IntArrayLookup reverseLookup = reverse.toDenseLookup();
        Relation.RelationProjection projection = new Relation.RelationProjection(
                forwardLookup.keys(),
                reverseLookup.keys(),
                forwardLookup.keys(),
                forwardLookup.values(),
                reverseLookup.keys(),
                reverseLookup.values());
        if (projection.isEmpty()) {
            return null;
        }
        return new CompiledComponent(Relation.binary(left, right, description, projection), resultCount);
    }

    public record CompilationStats(long queryNanos, long mappingNanos) {
        public long totalNanos() {
            return queryNanos + mappingNanos;
        }
    }

    private record CompiledComponent(Relation binding, long count) {
    }

    private static final class IntAccumulator {
        private int[] data;
        private int size = 0;

        IntAccumulator() {
            this(8);
        }

        IntAccumulator(int expected) {
            int capacity = Math.max(8, expected);
            data = new int[capacity];
        }

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

    /**
     * Primitive int-to-accumulator map using open addressing.
     * This avoids boxing during mapping and only grows (no removals).
     */
    private static final class IntAccumulatorMap {
        private static final float LOAD_FACTOR = 0.7f;
        private int mask;
        private int size;
        private int threshold;
        private int[] keys;
        private IntAccumulator[] values;
        private byte[] states;

        IntAccumulatorMap() {
            init(16);
        }

        boolean isEmpty() {
            return size == 0;
        }

        void add(int key, int value) {
            int slot = findSlot(key);
            if (states[slot] == 0) {
                if (size + 1 > threshold) {
                    resize();
                    slot = findSlot(key);
                }
                states[slot] = 1;
                keys[slot] = key;
                values[slot] = new IntAccumulator();
                size++;
            }
            values[slot].add(value);
        }

        int[] sortedKeys() {
            if (size == 0) {
                return EMPTY_INT_ARRAY;
            }
            int[] out = new int[size];
            int idx = 0;
            for (int i = 0; i < states.length; i++) {
                if (states[i] != 0) {
                    out[idx++] = keys[i];
                }
            }
            Arrays.sort(out);
            return out;
        }

        IntArrayLookup toDenseLookup() {
            int[] sorted = sortedKeys();
            if (sorted.length == 0) {
                return new IntArrayLookup(EMPTY_INT_ARRAY, EMPTY_INT_MATRIX);
            }
            int[][] domains = new int[sorted.length][];
            for (int i = 0; i < sorted.length; i++) {
                IntAccumulator accumulator = accumulatorFor(sorted[i]);
                if (accumulator == null) {
                    throw new IllegalStateException("Missing accumulator for key " + sorted[i]);
                }
                domains[i] = accumulator.toSortedDistinctArray();
            }
            return new IntArrayLookup(sorted, domains);
        }

        private IntAccumulator accumulatorFor(int key) {
            int slot = findSlot(key);
            if (states[slot] == 0 || keys[slot] != key) {
                return null;
            }
            return values[slot];
        }

        private void init(int capacity) {
            mask = capacity - 1;
            keys = new int[capacity];
            values = new IntAccumulator[capacity];
            states = new byte[capacity];
            threshold = (int) (capacity * LOAD_FACTOR);
            size = 0;
        }

        private int findSlot(int key) {
            int slot = mixHash(key) & mask;
            while (states[slot] != 0) {
                if (keys[slot] == key) {
                    return slot;
                }
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private void resize() {
            int oldCapacity = states.length;
            int[] oldKeys = keys;
            IntAccumulator[] oldValues = values;
            byte[] oldStates = states;

            init(oldCapacity * 2);
            for (int i = 0; i < oldCapacity; i++) {
                if (oldStates[i] == 0) {
                    continue;
                }
                int slot = findSlot(oldKeys[i]);
                states[slot] = 1;
                keys[slot] = oldKeys[i];
                values[slot] = oldValues[i];
                size++;
            }
        }

        private static int mixHash(int value) {
            int h = value;
            h ^= (h >>> 16);
            h *= 0x7feb352d;
            h ^= (h >>> 15);
            h *= 0x846ca68b;
            h ^= (h >>> 16);
            return h;
        }
    }

    private record IntArrayLookup(int[] keys, int[][] values) {
    }
}
