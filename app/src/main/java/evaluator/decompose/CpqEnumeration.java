package evaluator.decompose;

import evaluator.decompose.CpqDecomposition.Component;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;

public final class CpqEnumeration {
    private CpqEnumeration() {
    }

    public static List<Component> enumerateCpqkComponents(CQ cq, int k) {
        Objects.requireNonNull(cq, "cq");
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }

        Enumerator enumerator = new Enumerator(cq, k);
        return enumerator.enumerate();
    }

    public static List<List<Component>> enumerateExactDecompositions(CQ cq, int k, int limit) {
        List<List<Component>> out = new ArrayList<>();
        enumerateExactDecompositions(cq, k, limit, out::add);
        return out;
    }

    public static void enumerateExactDecompositions(CQ cq, int k, int limit, Consumer<List<Component>> out) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(out, "out");
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }

        Instance instance = Instance.from(cq);
        List<Component> components = enumerateCpqkComponents(cq, k);
        int edgeCount = instance.edges().size();
        List<VarCQ> vertices = instance.vertices();
        Map<VarCQ, Integer> varIndex = new HashMap<>(vertices.size());
        for (int i = 0; i < vertices.size(); i++) {
            varIndex.put(vertices.get(i), i);
        }

        List<BitSet> componentVars = new ArrayList<>(components.size());
        List<BitSet> componentEndpoints = new ArrayList<>(components.size());
        for (Component component : components) {
            BitSet vars = new BitSet(vertices.size());
            BitSet endpoints = new BitSet(vertices.size());
            Integer sIdx = varIndex.get(component.s());
            if (sIdx != null) {
                endpoints.set(sIdx);
            }
            Integer tIdx = varIndex.get(component.t());
            if (tIdx != null) {
                endpoints.set(tIdx);
            }
            BitSet mask = component.mask();
            for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
                Edge edge = instance.edges().get(e);
                vars.set(varIndex.get(edge.src()));
                vars.set(varIndex.get(edge.trg()));
            }
            componentVars.add(vars);
            componentEndpoints.add(endpoints);
        }

        List<List<Integer>> cover = new ArrayList<>(edgeCount);
        for (int e = 0; e < edgeCount; e++) {
            cover.add(new ArrayList<>());
        }
        for (int idx = 0; idx < components.size(); idx++) {
            BitSet mask = components.get(idx).mask();
            for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
                cover.get(e).add(idx);
            }
        }
        for (int e = 0; e < edgeCount; e++) {
            Edge edge = instance.edges().get(e);
            cover.get(e).sort(Comparator
                    .comparingInt((Integer idx) -> components.get(idx).mask().cardinality())
                    .thenComparingInt(idx -> components.get(idx).diameter())
                    .thenComparingInt(idx -> prefersEdgeDirection(components.get(idx), edge))
                    .thenComparingInt(idx -> idx));
        }

        Set<VarCQ> isolated = isolatedFreeVars(instance.freeVars(), instance.edges());
        Set<VarCQ> required = new HashSet<>(instance.freeVars());
        required.removeAll(isolated);

        int max = limit == 0 ? Integer.MAX_VALUE : limit;
        BitSet empty = new BitSet(edgeCount);

        class Dfs {
            int outputs = 0;

            void run(BitSet covered, Set<VarCQ> freeCovered, ArrayList<Integer> chosen) {
                if (outputs >= max) {
                    return;
                }

                int nextEdge = covered.nextClearBit(0);
                if (nextEdge >= edgeCount) {
                    if (!endpointExposureSatisfied(chosen, componentVars, componentEndpoints, vertices.size())) {
                        return;
                    }

                    if (!freeCovered.containsAll(required)) {
                        return;
                    }

                    Set<VarCQ> missing = new HashSet<>(instance.freeVars());
                    missing.removeAll(freeCovered);
                    if (!isolated.containsAll(missing)) {
                        return;
                    }

                    ArrayList<Integer> ordered = new ArrayList<>(chosen);
                    ordered.sort(Integer::compareTo);
                    ArrayList<Component> decomposition = new ArrayList<>(ordered.size() + missing.size());
                    for (int idx : ordered) {
                        decomposition.add(components.get(idx));
                    }
                    for (VarCQ v : missing) {
                        decomposition.add(new Component(v, v, 0, empty, CPQ.id(), "id"));
                    }

                    out.accept(decomposition);
                    outputs++;
                    return;
                }

                for (int idx : cover.get(nextEdge)) {
                    Component c = components.get(idx);
                    if (c.mask().intersects(covered)) {
                        continue;
                    }

                    BitSet nextCovered = (BitSet) covered.clone();
                    nextCovered.or(c.mask());

                    Set<VarCQ> nextFreeCovered = new HashSet<>(freeCovered);
                    if (required.contains(c.s())) {
                        nextFreeCovered.add(c.s());
                    }
                    if (required.contains(c.t())) {
                        nextFreeCovered.add(c.t());
                    }

                    chosen.add(idx);
                    run(nextCovered, nextFreeCovered, chosen);
                    chosen.remove(chosen.size() - 1);

                    if (outputs >= max) {
                        return;
                    }
                }
            }
        }

        new Dfs().run(new BitSet(edgeCount), new HashSet<>(), new ArrayList<>());
    }

    private static int prefersEdgeDirection(Component component, Edge edge) {
        return component.s().equals(edge.src()) && component.t().equals(edge.trg()) ? 0 : 1;
    }

    private static Set<VarCQ> isolatedFreeVars(Set<VarCQ> freeVars, List<Edge> edges) {
        Set<VarCQ> incident = new HashSet<>();
        for (Edge e : edges) {
            incident.add(e.src());
            incident.add(e.trg());
        }

        Set<VarCQ> isolated = new HashSet<>(freeVars);
        isolated.removeAll(incident);
        return isolated;
    }

    private static boolean endpointExposureSatisfied(
            List<Integer> chosen,
            List<BitSet> componentVars,
            List<BitSet> componentEndpoints,
            int varCount) {
        if (varCount == 0 || chosen.size() < 2) {
            return true;
        }

        int[] counts = new int[varCount];
        for (int idx : chosen) {
            BitSet vars = componentVars.get(idx);
            for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
                counts[v]++;
            }
        }

        BitSet shared = new BitSet(varCount);
        for (int v = 0; v < varCount; v++) {
            if (counts[v] >= 2) {
                shared.set(v);
            }
        }
        if (shared.isEmpty()) {
            return true;
        }

        for (int idx : chosen) {
            BitSet vars = componentVars.get(idx);
            if (!vars.intersects(shared)) {
                continue;
            }
            BitSet requiredEndpoints = (BitSet) vars.clone();
            requiredEndpoints.and(shared);
            requiredEndpoints.andNot(componentEndpoints.get(idx));
            if (!requiredEndpoints.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // ==================== Core Enumerator ====================

    private static final class Enumerator {
        private final int maxCoreDiam;
        private final List<Edge> edges;
        private final int atomCount;
        private int nextId;
        private final Map<ComponentKey, InternalComponent> bestByKey = new HashMap<>();

        private final Map<VarCQ, List<InternalComponent>> bySource = new HashMap<>();
        private final Map<VarCQ, List<InternalComponent>> byTarget = new HashMap<>();
        private final Map<EndpointPair, List<InternalComponent>> byEndpoints = new HashMap<>();

        Enumerator(CQ cq, int k) {
            this.maxCoreDiam = k;
            Instance instance = Instance.from(cq);
            this.edges = instance.edges();
            this.atomCount = edges.size();
            this.nextId = 0;
        }

        List<Component> enumerate() {
            Queue<InternalComponent> worklist = new ArrayDeque<>();

            initializeBaseComponents(worklist);

            while (!worklist.isEmpty()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Enumeration interrupted");
                }

                InternalComponent left = worklist.poll();
                if (!isCurrent(left)) {
                    continue;
                }

                // Concatenation (left ◦ right)
                List<InternalComponent> rightCandidates = bySource.getOrDefault(left.t(), List.of());
                for (int i = 0, size = rightCandidates.size(); i < size; i++) {
                    InternalComponent right = rightCandidates.get(i);
                    if (right.id() >= left.id() || !isCurrent(right)) {
                        continue;
                    }
                    tryConcat(left, right, worklist);
                }

                // Concatenation (parent ◦ left)
                List<InternalComponent> parentCandidates = byTarget.getOrDefault(left.s(), List.of());
                for (int i = 0, size = parentCandidates.size(); i < size; i++) {
                    InternalComponent parent = parentCandidates.get(i);
                    if (parent.id() > left.id() || !isCurrent(parent)) {
                        continue;
                    }
                    tryConcat(parent, left, worklist);
                }

                // Intersection (left ∩ other)
                EndpointPair endpoints = new EndpointPair(left.s(), left.t());
                List<InternalComponent> parallelCandidates = byEndpoints.getOrDefault(endpoints, List.of());
                for (int i = 0, size = parallelCandidates.size(); i < size; i++) {
                    InternalComponent other = parallelCandidates.get(i);
                    if (other.id() >= left.id() || !isCurrent(other)) {
                        continue;
                    }
                    tryIntersect(left, other, worklist);
                }

            }

            List<InternalComponent> values = new ArrayList<>(bestByKey.values());
            values.sort(Comparator.comparingInt(InternalComponent::coreDiam).thenComparing(InternalComponent::canonical));

            List<Component> result = new ArrayList<>(values.size());
            for (InternalComponent component : values) {
                result.add(toExternalComponent(component));
            }
            result.sort(Comparator.comparingInt(Component::diameter).thenComparing(Component::canonical));

            if (Boolean.getBoolean("cpq.enumeration.dump")) {
                for (Component component : result) {
                    System.out.println(formatComponent(component));
                }
            }

            return result;
        }

        private void initializeBaseComponents(Queue<InternalComponent> worklist) {
            for (Edge e : edges) {
                BitSet owned = new BitSet(atomCount);
                owned.set(e.id());
                BitSet inverse = new BitSet(atomCount);

                NormalizedData fwdNorm = normalize(CPQ.label(e.label()));
                InternalComponent fwd = newComponent(
                        e.src(),
                        e.trg(),
                        (BitSet) owned.clone(),
                        (BitSet) inverse.clone(),
                        1,
                        fwdNorm.cpq(),
                        fwdNorm.canonical(),
                        fwdNorm.size());
                registerIfBetter(fwd, worklist);

                Predicate inv = e.label().getInverse();
                NormalizedData invNorm = normalize(CPQ.label(inv));
                BitSet inverseBack = new BitSet(atomCount);
                inverseBack.set(e.id());
                InternalComponent bwd = newComponent(
                        e.trg(),
                        e.src(),
                        (BitSet) owned.clone(),
                        (BitSet) inverseBack.clone(),
                        1,
                        invNorm.cpq(),
                        invNorm.canonical(),
                        invNorm.size());
                registerIfBetter(bwd, worklist);
            }
        }

        private void tryConcat(InternalComponent left, InternalComponent right, Queue<InternalComponent> worklist) {
            if (!left.t().equals(right.s())) {
                return;
            }
            if (left.ownedAtoms().intersects(right.ownedAtoms())) {
                return;
            }

            int newCore = left.coreDiam() + right.coreDiam();
            if (newCore > maxCoreDiam) {
                return;
            }
            BitSet owned = union(left.ownedAtoms(), right.ownedAtoms());
            BitSet inverse = union(left.inverseAtoms(), right.inverseAtoms());
            CPQ cpq = CPQ.concat(List.of(left.cpq(), right.cpq()));
            NormalizedData norm = normalize(cpq);

            InternalComponent out = newComponent(
                    left.s(),
                    right.t(),
                    owned,
                    inverse,
                    newCore,
                    norm.cpq(),
                    norm.canonical(),
                    norm.size());
            registerIfBetter(out, worklist);
        }

        private void tryIntersect(InternalComponent left, InternalComponent right, Queue<InternalComponent> worklist) {
            if (!left.s().equals(right.s()) || !left.t().equals(right.t())) {
                return;
            }
            if (left.ownedAtoms().intersects(right.ownedAtoms())) {
                return;
            }
            int newCore = Math.max(left.coreDiam(), right.coreDiam());
            if (newCore > maxCoreDiam) {
                return;
            }
            BitSet owned = union(left.ownedAtoms(), right.ownedAtoms());
            BitSet inverse = union(left.inverseAtoms(), right.inverseAtoms());
            CPQ cpq = CPQ.intersect(List.of(left.cpq(), right.cpq()));
            NormalizedData norm = normalize(cpq);

            InternalComponent out = newComponent(
                    left.s(),
                    left.t(),
                    owned,
                    inverse,
                    newCore,
                    norm.cpq(),
                    norm.canonical(),
                    norm.size());
            registerIfBetter(out, worklist);
        }

        private void registerIfBetter(InternalComponent c, Queue<InternalComponent> worklist) {
            InternalComponent existing = bestByKey.get(c.key());
            if (existing != null && !isBetter(c, existing)) {
                return;
            }

            bestByKey.put(c.key(), c);
            worklist.add(c);

            bySource.computeIfAbsent(c.s(), v -> new ArrayList<>()).add(c);
            byTarget.computeIfAbsent(c.t(), v -> new ArrayList<>()).add(c);
            byEndpoints.computeIfAbsent(new EndpointPair(c.s(), c.t()), v -> new ArrayList<>()).add(c);

        }

        private boolean isBetter(InternalComponent candidate, InternalComponent existing) {
            if (candidate.coreDiam() != existing.coreDiam()) {
                return candidate.coreDiam() < existing.coreDiam();
            }
            if (candidate.size() != existing.size()) {
                return candidate.size() < existing.size();
            }
            return false;
        }

        private Component toExternalComponent(InternalComponent component) {
            return new Component(
                    component.s(),
                    component.t(),
                    component.coreDiam(),
                    component.ownedAtoms(),
                    component.cpq(),
                    component.canonical());
        }

        private NormalizedData normalize(CPQ cpq) {
            CpqNormalizer.Normalized norm = CpqNormalizer.normalize(cpq);
            CPQ normalized = norm.cpq();
            String canonical = norm.canonical();
            int size = cpqSize(normalized);
            return new NormalizedData(normalized, canonical, size);
        }

        private InternalComponent newComponent(
                VarCQ s,
                VarCQ t,
                BitSet ownedAtoms,
                BitSet inverseAtoms,
                int coreDiam,
                CPQ cpq,
                String canonical,
                int size) {
            ComponentKey key = new ComponentKey(s, t, ownedAtoms, inverseAtoms);
            return new InternalComponent(nextId++, s, t, ownedAtoms, inverseAtoms, coreDiam, cpq, canonical, size, key);
        }

        private boolean isCurrent(InternalComponent component) {
            return bestByKey.get(component.key()) == component;
        }
    }

    private static BitSet union(BitSet left, BitSet right) {
        BitSet out = (BitSet) left.clone();
        out.or(right);
        return out;
    }

    private static int cpqSize(CPQ cpq) {
        return countNodes(cpq.toAbstractSyntaxTree());
    }

    private static int countNodes(dev.roanh.gmark.ast.QueryTree node) {
        int total = 1;
        for (int i = 0; i < node.getArity(); i++) {
            total += countNodes(node.getOperand(i));
        }
        return total;
    }

    private static String formatComponent(Component component) {
        return "component s=" + component.s().getName()
                + " t=" + component.t().getName()
                + " diam=" + component.diameter()
                + " mask=" + component.mask()
                + " canonical=" + component.canonical();
    }

    private record NormalizedData(CPQ cpq, String canonical, int size) {
    }

    private record InternalComponent(
            int id,
            VarCQ s,
            VarCQ t,
            BitSet ownedAtoms,
            BitSet inverseAtoms,
            int coreDiam,
            CPQ cpq,
            String canonical,
            int size,
            ComponentKey key) {
    }

    private record ComponentKey(VarCQ s, VarCQ t, BitSet ownedAtoms, BitSet inverseAtoms) {
        private ComponentKey {
            ownedAtoms = (BitSet) ownedAtoms.clone();
            inverseAtoms = (BitSet) inverseAtoms.clone();
        }
    }

    private record EndpointPair(VarCQ s, VarCQ t) {
    }

    private record Edge(int id, VarCQ src, VarCQ trg, Predicate label) {
    }

    private record Instance(List<VarCQ> vertices, List<Edge> edges, Set<VarCQ> freeVars) {
        static Instance from(CQ cq) {
            UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
            List<VarCQ> vertices = graph.getNodes().stream()
                    .map(node -> node.getData())
                    .sorted(Comparator.comparing(VarCQ::getName))
                    .toList();

            List<Edge> edges = new ArrayList<>();
            List<UniqueGraph.GraphEdge<VarCQ, AtomCQ>> graphEdges = graph.getEdges();
            for (int i = 0; i < graphEdges.size(); i++) {
                AtomCQ atom = graphEdges.get(i).getData();
                edges.add(new Edge(i, atom.getSource(), atom.getTarget(), atom.getLabel()));
            }

            return new Instance(vertices, edges, cq.getFreeVariables());
        }
    }
}
