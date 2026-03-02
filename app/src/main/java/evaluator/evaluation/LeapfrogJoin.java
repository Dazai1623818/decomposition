package evaluator.evaluation;

import evaluator.evaluation.Relation.DomainAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class LeapfrogJoin {
    private static final int FIRST = 0;
    private static final boolean DEFAULT_SAFE_DISTINCT_FAST_PATH = true;

    private LeapfrogJoin() {
    }

    public enum JoinMode {
        PROJECTED_ROWS(true),
        PROJECTED_COUNT(false);

        private final boolean rows;

        JoinMode(boolean rows) {
            this.rows = rows;
        }

        boolean collectsRows() {
            return rows;
        }

        boolean countsOnly() {
            return !rows;
        }
    }

    public sealed interface JoinResult permits JoinResult.Rows, JoinResult.Count {
        record Rows(List<Map<String, Integer>> rows) implements JoinResult {
        }

        record Count(long count) implements JoinResult {
        }
    }

    public static JoinResult join(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projectedVars,
            JoinMode mode) {
        return join(relations, variableOrder, projectedVars, mode, DEFAULT_SAFE_DISTINCT_FAST_PATH);
    }

    public static JoinResult join(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projectedVars,
            JoinMode mode,
            boolean safeDistinctFastPath) {
        Objects.requireNonNull(relations, "relations");
        Objects.requireNonNull(variableOrder, "variableOrder");
        Objects.requireNonNull(projectedVars, "projectedVars");
        Objects.requireNonNull(mode, "mode");
        if (relations.isEmpty()) {
            return emptyResult(mode);
        }

        Map<String, List<Relation>> bindingsByVar = buildBindingsByVar(relations);
        return joinProjected(bindingsByVar, variableOrder, projectedVars, mode, safeDistinctFastPath);
    }

    private static JoinResult emptyResult(JoinMode mode) {
        return result(mode, List.of(), 0L);
    }

    private static JoinResult result(JoinMode mode, List<Map<String, Integer>> rows, long count) {
        return mode.collectsRows() ? new JoinResult.Rows(rows) : new JoinResult.Count(count);
    }

    private static Map<String, Integer> toRow(List<String> variables, int[] values) {
        Map<String, Integer> row = new LinkedHashMap<>(variables.size());
        for (int i = 0; i < variables.size(); i++) {
            row.put(variables.get(i), values[i]);
        }
        return row;
    }

    private static int[] projectedIndices(List<String> projectedVars, Map<String, Integer> indexByVar) {
        int[] projectedIndices = new int[projectedVars.size()];
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < projectedVars.size(); i++) {
            String var = projectedVars.get(i);
            if (!seen.add(var)) {
                throw new IllegalArgumentException("Duplicate projected variable " + var);
            }
            Integer idx = indexByVar.get(var);
            if (idx == null) {
                throw new IllegalArgumentException("Unknown projected variable " + var);
            }
            projectedIndices[i] = idx;
        }
        return projectedIndices;
    }

    private static Map<String, List<Relation>> buildBindingsByVar(List<Relation> relations) {
        Map<String, List<Relation>> bindingsByVar = new HashMap<>();
        for (Relation binding : relations) {
            binding.register(bindingsByVar);
        }
        return bindingsByVar;
    }

    private static JoinResult joinProjected(
            Map<String, List<Relation>> bindingsByVar,
            List<String> variableOrder,
            List<String> projectedVars,
            JoinMode mode,
            boolean safeDistinctFastPath) {
        if (projectedVars.isEmpty()) {
            return result(mode, List.of(), 0L);
        }

        boolean collectRows = mode.collectsRows();
        ProjectedPlan plan = buildProjectedPlan(variableOrder, projectedVars);
        Map<String, Integer> indexByVar = plan.indexByVar();
        int[] assignment = new int[variableOrder.size()];
        boolean[] bound = new boolean[variableOrder.size()];
        int[] projectedIndices = plan.projectedIndices();
        int projectedDepth = plan.projectedDepth();
        boolean needsExtensionCheck = plan.needsExtensionCheck();
        int stopDepth = plan.stopDepth();
        DomainAccessor[][] constraintsByDepth = compileConstraintsByDepth(variableOrder, bindingsByVar, indexByVar);
        boolean requiresDistinct = requiresDistinctTracking(plan, safeDistinctFastPath);
        boolean materializeProjection = collectRows || requiresDistinct;

        List<Map<String, Integer>> rows = collectRows ? new ArrayList<>() : List.of();
        long[] count = new long[] { 0L };
        IntHashSet seenSingles = requiresDistinct && projectedIndices.length == 1 ? new IntHashSet() : null;
        IntPairHashSet seenPairs = requiresDistinct && projectedIndices.length == 2 ? new IntPairHashSet() : null;
        IntTupleHashSet seenTuples = requiresDistinct && projectedIndices.length > 2
                ? new IntTupleHashSet(projectedIndices.length)
                : null;
        int[] keyBuffer = materializeProjection ? new int[projectedIndices.length] : null;
        CursorWorkspace[] workspaces = initWorkspaces(constraintsByDepth);

        searchPrefix(
                variableOrder,
                0,
                stopDepth,
                constraintsByDepth,
                assignment,
                bound,
                workspaces,
                (prefixAssignment, prefixBound) -> {
                    if (materializeProjection) {
                        for (int i = 0; i < projectedIndices.length; i++) {
                            keyBuffer[i] = prefixAssignment[projectedIndices[i]];
                        }
                    }
                    if (needsExtensionCheck) {
                        if (requiresDistinct && seenContains(
                                seenSingles,
                                seenPairs,
                                seenTuples,
                                keyBuffer)) {
                            return true;
                        }
                        if (!existsExtension(
                                variableOrder,
                                projectedDepth,
                                constraintsByDepth,
                                prefixAssignment,
                                prefixBound,
                                workspaces)) {
                            return true;
                        }
                        // Only dedupe successful projected tuples. A failed extension for one
                        // prefix binding must not suppress another prefix that can extend.
                        if (requiresDistinct && !seenAdd(
                                seenSingles,
                                seenPairs,
                                seenTuples,
                                keyBuffer)) {
                            return true;
                        }
                    } else if (requiresDistinct && !seenAdd(
                            seenSingles,
                            seenPairs,
                            seenTuples,
                            keyBuffer)) {
                        return true;
                    }
                    if (collectRows) {
                        rows.add(toRow(projectedVars, keyBuffer));
                    } else {
                        count[0]++;
                    }
                    return true;
                });

        return result(mode, rows, count[0]);
    }

    private static boolean seenContains(
            IntHashSet seenSingles,
            IntPairHashSet seenPairs,
            IntTupleHashSet seenTuples,
            int[] tuple) {
        if (seenSingles != null) {
            return seenSingles.contains(tuple[0]);
        }
        if (seenPairs != null) {
            return seenPairs.contains(tuple[0], tuple[1]);
        }
        if (seenTuples != null) {
            return seenTuples.contains(tuple);
        }
        return false;
    }

    private static boolean seenAdd(
            IntHashSet seenSingles,
            IntPairHashSet seenPairs,
            IntTupleHashSet seenTuples,
            int[] tuple) {
        if (seenSingles != null) {
            return seenSingles.add(tuple[0]);
        }
        if (seenPairs != null) {
            return seenPairs.add(tuple[0], tuple[1]);
        }
        if (seenTuples != null) {
            return seenTuples.add(tuple);
        }
        return true;
    }

    private static final class IntHashSet {
        private static final float LOAD_FACTOR = 0.7f;
        private int mask;
        private int size;
        private int threshold;
        private int[] table;
        private byte[] states;

        IntHashSet() {
            init(16);
        }

        boolean add(int value) {
            if (size + 1 > threshold) {
                resize();
            }
            int slot = mixHash(value) & mask;
            while (states[slot] != 0) {
                if (table[slot] == value) {
                    return false;
                }
                slot = (slot + 1) & mask;
            }
            states[slot] = 1;
            table[slot] = value;
            size++;
            return true;
        }

        boolean contains(int value) {
            int slot = mixHash(value) & mask;
            while (states[slot] != 0) {
                if (table[slot] == value) {
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            return false;
        }

        private void init(int capacity) {
            mask = capacity - 1;
            table = new int[capacity];
            states = new byte[capacity];
            threshold = (int) (capacity * LOAD_FACTOR);
            size = 0;
        }

        private void resize() {
            int[] oldTable = table;
            byte[] oldStates = states;
            int oldCapacity = oldStates.length;
            init(oldCapacity * 2);
            for (int i = 0; i < oldCapacity; i++) {
                if (oldStates[i] == 0) {
                    continue;
                }
                int slot = mixHash(oldTable[i]) & mask;
                while (states[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                states[slot] = 1;
                table[slot] = oldTable[i];
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

    private static final class IntPairHashSet {
        private static final float LOAD_FACTOR = 0.7f;
        private int mask;
        private int size;
        private int threshold;
        private long[] table;
        private byte[] states;

        IntPairHashSet() {
            init(16);
        }

        boolean add(int first, int second) {
            if (size + 1 > threshold) {
                resize();
            }
            long key = pack(first, second);
            int slot = mixHash(key) & mask;
            while (states[slot] != 0) {
                if (table[slot] == key) {
                    return false;
                }
                slot = (slot + 1) & mask;
            }
            states[slot] = 1;
            table[slot] = key;
            size++;
            return true;
        }

        boolean contains(int first, int second) {
            long key = pack(first, second);
            int slot = mixHash(key) & mask;
            while (states[slot] != 0) {
                if (table[slot] == key) {
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            return false;
        }

        private static long pack(int first, int second) {
            return ((long) first << 32) ^ (second & 0xFFFFFFFFL);
        }

        private void init(int capacity) {
            mask = capacity - 1;
            table = new long[capacity];
            states = new byte[capacity];
            threshold = (int) (capacity * LOAD_FACTOR);
            size = 0;
        }

        private void resize() {
            long[] oldTable = table;
            byte[] oldStates = states;
            int oldCapacity = oldStates.length;
            init(oldCapacity * 2);
            for (int i = 0; i < oldCapacity; i++) {
                if (oldStates[i] == 0) {
                    continue;
                }
                int slot = mixHash(oldTable[i]) & mask;
                while (states[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                states[slot] = 1;
                table[slot] = oldTable[i];
                size++;
            }
        }

        private static int mixHash(long value) {
            long h = value;
            h ^= (h >>> 33);
            h *= 0xff51afd7ed558ccdL;
            h ^= (h >>> 33);
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= (h >>> 33);
            return (int) h;
        }
    }

    private static Map<String, Integer> buildIndexByVar(List<String> order) {
        Map<String, Integer> indexByVar = new HashMap<>(order.size());
        for (int i = 0; i < order.size(); i++) {
            indexByVar.put(order.get(i), i);
        }
        return indexByVar;
    }

    private record ProjectedPlan(
            Map<String, Integer> indexByVar,
            int[] projectedIndices,
            int projectedDepth,
            boolean projectedPrefixOnly,
            boolean needsExtensionCheck,
            int stopDepth) {
    }

    private static ProjectedPlan buildProjectedPlan(List<String> variableOrder, List<String> projectedVars) {
        Map<String, Integer> indexByVar = buildIndexByVar(variableOrder);
        int[] projectedIndices = projectedIndices(projectedVars, indexByVar);
        int projectedDepth = maxIndex(projectedIndices) + 1;
        Set<String> projected = new HashSet<>(projectedVars);
        boolean projectedPrefixOnly = true;
        for (int i = 0; i < projectedDepth; i++) {
            if (!projected.contains(variableOrder.get(i))) {
                projectedPrefixOnly = false;
                break;
            }
        }
        boolean needsExtensionCheck = projectedDepth < variableOrder.size();
        int stopDepth = needsExtensionCheck ? projectedDepth : variableOrder.size();
        return new ProjectedPlan(
                indexByVar,
                projectedIndices,
                projectedDepth,
                projectedPrefixOnly,
                needsExtensionCheck,
                stopDepth);
    }

    private static boolean requiresDistinctTracking(ProjectedPlan plan, boolean safeDistinctFastPath) {
        if (!safeDistinctFastPath) {
            return true;
        }
        // If projected variables already form an order prefix, each projected tuple
        // is visited at most once by construction.
        return !plan.projectedPrefixOnly();
    }

    private static int maxIndex(int[] indices) {
        int max = -1;
        for (int idx : indices) {
            if (idx > max) {
                max = idx;
            }
        }
        return max;
    }

    private interface PrefixAction {
        boolean handle(int[] assignment, boolean[] bound);
    }

    private static boolean searchPrefix(
            List<String> order,
            int depth,
            int stopDepth,
            DomainAccessor[][] constraintsByDepth,
            int[] assignment,
            boolean[] bound,
            CursorWorkspace[] workspaces,
            PrefixAction action) {
        checkInterrupted();
        if (depth == stopDepth) {
            return action.handle(assignment, bound);
        }

        int variableIndex = depth;
        DomainAccessor[] constraints = constraintsByDepth[depth];
        if (constraints.length == 0) {
            return true;
        }
        if (constraints.length == 1) {
            int[] domain = constraints[FIRST].domain(assignment, bound);
            for (int value : domain) {
                checkInterrupted();
                assignment[variableIndex] = value;
                bound[variableIndex] = true;
                if (!searchPrefix(
                        order,
                        depth + 1,
                        stopDepth,
                        constraintsByDepth,
                        assignment,
                        bound,
                        workspaces,
                        action)) {
                    bound[variableIndex] = false;
                    return false;
                }
                bound[variableIndex] = false;
            }
            return true;
        }

        CursorWorkspace workspace = workspaces[depth];
        if (!resetWorkspaceCursors(workspace, constraints, assignment, bound)) {
            return true;
        }

        LeapfrogIterator iterator = workspace.iterator();
        iterator.init();
        while (!iterator.atEnd()) {
            checkInterrupted();
            assignment[variableIndex] = iterator.key();
            bound[variableIndex] = true;
            if (!searchPrefix(
                    order,
                    depth + 1,
                    stopDepth,
                    constraintsByDepth,
                    assignment,
                    bound,
                    workspaces,
                    action)) {
                bound[variableIndex] = false;
                return false;
            }
            bound[variableIndex] = false;
            iterator.next();
        }
        return true;
    }

    private static boolean existsExtension(
            List<String> order,
            int depth,
            DomainAccessor[][] constraintsByDepth,
            int[] assignment,
            boolean[] bound,
            CursorWorkspace[] workspaces) {

        checkInterrupted();
        if (depth == order.size()) {
            return true;
        }

        int variableIndex = depth;
        DomainAccessor[] constraints = constraintsByDepth[depth];
        if (constraints.length == 0) {
            return false;
        }
        if (constraints.length == 1) {
            int[] domain = constraints[FIRST].domain(assignment, bound);
            for (int value : domain) {
                checkInterrupted();
                assignment[variableIndex] = value;
                bound[variableIndex] = true;
                if (existsExtension(order, depth + 1, constraintsByDepth, assignment, bound, workspaces)) {
                    bound[variableIndex] = false;
                    return true;
                }
                bound[variableIndex] = false;
            }
            return false;
        }

        CursorWorkspace workspace = workspaces[depth];
        if (!resetWorkspaceCursors(workspace, constraints, assignment, bound)) {
            return false;
        }

        LeapfrogIterator iterator = workspace.iterator();
        iterator.init();
        while (!iterator.atEnd()) {
            checkInterrupted();
            assignment[variableIndex] = iterator.key();
            bound[variableIndex] = true;
            if (existsExtension(order, depth + 1, constraintsByDepth, assignment, bound, workspaces)) {
                bound[variableIndex] = false;
                return true;
            }
            bound[variableIndex] = false;
            iterator.next();
        }
        return false;
    }

    private static final class IntTupleHashSet {
        private static final float LOAD_FACTOR = 0.7f;
        private final int tupleSize;
        private int mask;
        private int size;
        private int threshold;
        private int[] table;
        private byte[] states;

        IntTupleHashSet(int tupleSize) {
            if (tupleSize <= 0) {
                throw new IllegalArgumentException("tupleSize must be > 0");
            }
            this.tupleSize = tupleSize;
            int capacity = 16;
            this.mask = capacity - 1;
            this.table = new int[capacity * tupleSize];
            this.states = new byte[capacity];
            this.threshold = (int) (capacity * LOAD_FACTOR);
        }

        boolean add(int[] tuple) {
            if (tuple.length != tupleSize) {
                throw new IllegalArgumentException("tuple length mismatch");
            }
            if (size + 1 > threshold) {
                resize();
            }
            int hash = mixHash(tuple);
            int slot = hash & mask;
            while (states[slot] != 0) {
                if (matches(slot, tuple)) {
                    return false;
                }
                slot = (slot + 1) & mask;
            }
            store(slot, tuple);
            return true;
        }

        boolean contains(int[] tuple) {
            if (tuple.length != tupleSize) {
                throw new IllegalArgumentException("tuple length mismatch");
            }
            int hash = mixHash(tuple);
            int slot = hash & mask;
            while (states[slot] != 0) {
                if (matches(slot, tuple)) {
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            return false;
        }

        private void store(int slot, int[] tuple) {
            int base = slot * tupleSize;
            System.arraycopy(tuple, 0, table, base, tupleSize);
            states[slot] = 1;
            size++;
        }

        private boolean matches(int slot, int[] tuple) {
            int base = slot * tupleSize;
            for (int i = 0; i < tupleSize; i++) {
                if (table[base + i] != tuple[i]) {
                    return false;
                }
            }
            return true;
        }

        private int mixHash(int[] tuple) {
            int h = 1;
            for (int value : tuple) {
                h = 31 * h + value;
            }
            h ^= (h >>> 16);
            h *= 0x7feb352d;
            h ^= (h >>> 15);
            h *= 0x846ca68b;
            h ^= (h >>> 16);
            return h;
        }

        private void resize() {
            int oldCapacity = states.length;
            int[] oldTable = table;
            byte[] oldStates = states;

            int newCapacity = oldCapacity * 2;
            mask = newCapacity - 1;
            table = new int[newCapacity * tupleSize];
            states = new byte[newCapacity];
            threshold = (int) (newCapacity * LOAD_FACTOR);
            size = 0;

            for (int slot = 0; slot < oldCapacity; slot++) {
                if (oldStates[slot] == 0) {
                    continue;
                }
                int oldBase = slot * tupleSize;
                int rehash = mixHash(oldTable, oldBase);
                int newSlot = rehash & mask;
                while (states[newSlot] != 0) {
                    newSlot = (newSlot + 1) & mask;
                }
                int newBase = newSlot * tupleSize;
                System.arraycopy(oldTable, oldBase, table, newBase, tupleSize);
                states[newSlot] = 1;
                size++;
            }
        }

        private int mixHash(int[] tupleTable, int base) {
            int h = 1;
            for (int i = 0; i < tupleSize; i++) {
                h = 31 * h + tupleTable[base + i];
            }
            h ^= (h >>> 16);
            h *= 0x7feb352d;
            h ^= (h >>> 15);
            h *= 0x846ca68b;
            h ^= (h >>> 16);
            return h;
        }

        int size() {
            return size;
        }
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new JoinInterruptedException();
        }
    }

    private static CursorWorkspace[] initWorkspaces(
            DomainAccessor[][] constraintsByDepth) {
        CursorWorkspace[] workspaces = new CursorWorkspace[constraintsByDepth.length];
        for (int depth = 0; depth < constraintsByDepth.length; depth++) {
            workspaces[depth] = new CursorWorkspace(constraintsByDepth[depth].length);
        }
        return workspaces;
    }

    private static DomainAccessor[][] compileConstraintsByDepth(
            List<String> order,
            Map<String, List<Relation>> bindingsByVar,
            Map<String, Integer> indexByVar) {
        DomainAccessor[][] constraintsByDepth = new DomainAccessor[order.size()][];
        for (int depth = 0; depth < order.size(); depth++) {
            String variable = order.get(depth);
            List<Relation> bindings = bindingsByVar.getOrDefault(variable, List.of());
            DomainAccessor[] constraints = new DomainAccessor[bindings.size()];
            for (int i = 0; i < bindings.size(); i++) {
                constraints[i] = bindings.get(i).compileForVariable(variable, indexByVar);
            }
            constraintsByDepth[depth] = constraints;
        }
        return constraintsByDepth;
    }

    private static boolean resetWorkspaceCursors(
            CursorWorkspace workspace,
            DomainAccessor[] constraints,
            int[] assignment,
            boolean[] bound) {
        int expectedCount = constraints.length;
        if (workspace.size() != expectedCount) {
            throw new IllegalStateException("Constraint count changed for workspace");
        }

        for (int i = 0; i < expectedCount; i++) {
            int[] domain = constraints[i].domain(assignment, bound);
            if (domain.length == 0) {
                return false;
            }
            workspace.cursors()[i].reset(domain);
        }
        return true;
    }

    private static final class JoinInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class CursorWorkspace {
        private final IntCursor[] cursors;
        private final LeapfrogIterator iterator;

        CursorWorkspace(int size) {
            this.cursors = new IntCursor[size];
            for (int i = 0; i < size; i++) {
                cursors[i] = new IntCursor();
            }
            this.iterator = size == 0 ? null : new LeapfrogIterator(cursors, size);
        }

        int size() {
            return cursors.length;
        }

        IntCursor[] cursors() {
            return cursors;
        }

        LeapfrogIterator iterator() {
            if (iterator == null) {
                throw new IllegalStateException("No iterator for empty workspace");
            }
            return iterator;
        }
    }

    private static final class LeapfrogIterator {
        private IntCursor[] cursors;
        private int size;
        private int pivot = 0;
        private boolean atEnd = false;

        LeapfrogIterator(IntCursor[] cursors, int size) {
            reset(cursors, size);
        }

        void reset(IntCursor[] cursors, int size) {
            this.cursors = Objects.requireNonNull(cursors, "cursors");
            if (size <= 0) {
                throw new IllegalArgumentException("cursors must not be empty");
            }
            if (size > cursors.length) {
                throw new IllegalArgumentException("size exceeds cursor array length");
            }
            this.size = size;
            this.pivot = 0;
            this.atEnd = false;
        }

        void init() {
            pivot = 0;
            atEnd = false;
            leapfrogSearch();
        }

        boolean atEnd() {
            return atEnd;
        }

        int key() {
            if (atEnd) {
                throw new IllegalStateException("atEnd");
            }
            return cursors[pivot].key();
        }

        void next() {
            if (atEnd) {
                return;
            }
            cursors[pivot].next();
            pivot = (pivot + 1) % size;
            leapfrogSearch();
        }

        private void leapfrogSearch() {
            while (true) {
                checkInterrupted();
                if (anyCursorAtEnd()) {
                    atEnd = true;
                    return;
                }
                int maxKey = maxKey();

                IntCursor cur = cursors[pivot];
                cur.seek(maxKey);
                if (cur.atEnd()) {
                    atEnd = true;
                    return;
                }

                if (cur.key() == maxKey) {
                    int minIndex = minIndex();
                    if (cursors[minIndex].key() == maxKey) {
                        return;
                    }
                    pivot = minIndex;
                } else {
                    pivot = (pivot + 1) % size;
                }
            }
        }

        private boolean anyCursorAtEnd() {
            for (int i = 0; i < size; i++) {
                if (cursors[i].atEnd()) {
                    return true;
                }
            }
            return false;
        }

        private int maxKey() {
            int maxKey = Integer.MIN_VALUE;
            for (int i = 0; i < size; i++) {
                IntCursor cursor = cursors[i];
                int key = cursor.key();
                if (key > maxKey) {
                    maxKey = key;
                }
            }
            return maxKey;
        }

        private int minIndex() {
            int minIndex = 0;
            int minKey = cursors[0].key();
            for (int i = 1; i < size; i++) {
                int key = cursors[i].key();
                if (key < minKey) {
                    minKey = key;
                    minIndex = i;
                }
            }
            return minIndex;
        }
    }

    private static final class IntCursor {
        private static final int[] EMPTY = new int[0];
        private int[] data = EMPTY;
        private int start = 0;
        private int end = 0;
        private int pos;

        IntCursor() {
        }

        IntCursor(int[] data) {
            this(data, 0, data.length);
        }

        IntCursor(int[] data, int start, int end) {
            reset(data, start, end);
        }

        void reset(int[] data) {
            reset(data, 0, data.length);
        }

        void reset(int[] data, int start, int end) {
            this.data = Objects.requireNonNull(data, "data");
            if (start < 0 || end < start || end > data.length) {
                throw new IllegalArgumentException("Invalid cursor bounds");
            }
            this.start = start;
            this.end = end;
            this.pos = start;
        }

        boolean atEnd() {
            return pos >= end;
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
            int idx = Arrays.binarySearch(data, pos, end, target);
            pos = idx >= 0 ? idx : -idx - 1;
        }
    }

}
