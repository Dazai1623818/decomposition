package evaluator.decomposition;

import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

final class SeriesParallelDecomposer {
    private SeriesParallelDecomposer() {
    }

    static Stream<Plan> decomposeSpqrGreedy(CQ cq) {
        return decomposeSpqrGreedy(cq, null);
    }

    static Stream<Plan> decomposeSpqrGreedy(CQ cq, java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(cq, "cq");
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        Biconnected biconnected = new Biconnected(query);
        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        terminals.addAll(biconnected.articulation());

        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter);
        List<Component> components = new ArrayList<>();
        for (List<Integer> block : biconnected.blocks()) {
            components.addAll(reducer.reduce(block, terminals));
        }
        return Stream.of(new Plan(query, components));
    }

    static Stream<Plan> decomposeGreedy(CQ cq) {
        return decomposeGreedy(cq, null);
    }

    static Stream<Plan> decomposeGreedy(CQ cq, java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(cq, "cq");
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        List<Integer> edgeIds = new ArrayList<>(query.edges().size());
        for (int i = 0; i < query.edges().size(); i++) {
            edgeIds.add(i);
        }

        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter);
        List<Component> components = reducer.reduce(edgeIds, terminals);
        return Stream.of(new Plan(query, components));
    }

    /**
     * Computes edge-biconnected components (blocks) and articulation vertices.
     * Every non-loop edge appears in exactly one block. Loop edges are returned
     * as singleton blocks.
     */
    private static final class Biconnected {
        private final List<VarCQ> vertices;
        private final Edge[] edges;
        private final ArrayList<Integer>[] adj;
        private final int[] disc;
        private final int[] low;
        private final boolean[] isArticulation;
        private final Deque<Integer> edgeStack;
        private final List<List<Integer>> blocks;
        private int time;

        Biconnected(ConjunctiveQuery query) {
            this.vertices = query.vertices();
            List<AtomCQ> atoms = query.edges();

            int n = vertices.size();
            int m = atoms.size();
            this.edges = new Edge[m];
            this.adj = initAdjacency(n);
            this.disc = new int[n];
            this.low = new int[n];
            this.isArticulation = new boolean[n];
            this.edgeStack = new ArrayDeque<>();
            this.blocks = new ArrayList<>();
            this.time = 0;

            Map<VarCQ, Integer> index = new HashMap<>(n);
            for (int i = 0; i < n; i++) {
                index.put(vertices.get(i), i);
            }

            List<Integer> loops = new ArrayList<>();
            for (int i = 0; i < m; i++) {
                AtomCQ atom = atoms.get(i);
                int u = index.get(atom.getSource());
                int v = index.get(atom.getTarget());
                Edge edge = new Edge(u, v);
                edges[i] = edge;
                if (edge.isLoop()) {
                    loops.add(i);
                    continue;
                }
                adj[u].add(i);
                adj[v].add(i);
            }

            for (int u = 0; u < n; u++) {
                if (disc[u] == 0) {
                    dfs(u, -1);
                    flushStack();
                }
            }

            for (int edgeId : loops) {
                ArrayList<Integer> block = new ArrayList<>(1);
                block.add(edgeId);
                blocks.add(block);
            }
        }

        List<List<Integer>> blocks() {
            return blocks;
        }

        Set<VarCQ> articulation() {
            Set<VarCQ> out = new HashSet<>();
            for (int i = 0; i < isArticulation.length; i++) {
                if (isArticulation[i]) {
                    out.add(vertices.get(i));
                }
            }
            return out;
        }

        private void dfs(int u, int parentEdge) {
            disc[u] = ++time;
            low[u] = disc[u];
            int childCount = 0;
            for (int edge : adj[u]) {
                int v = other(edge, u);
                if (disc[v] == 0) {
                    childCount++;
                    edgeStack.push(edge);
                    dfs(v, edge);
                    low[u] = Math.min(low[u], low[v]);
                    if (low[v] >= disc[u]) {
                        if (parentEdge != -1 || childCount > 1) {
                            isArticulation[u] = true;
                        }
                        popBlock(edge);
                    }
                } else if (edge != parentEdge && disc[v] < disc[u]) {
                    low[u] = Math.min(low[u], disc[v]);
                    edgeStack.push(edge);
                }
            }
        }

        private void popBlock(int stopEdge) {
            ArrayList<Integer> block = new ArrayList<>();
            int edge;
            do {
                edge = edgeStack.pop();
                block.add(edge);
            } while (edge != stopEdge);
            blocks.add(block);
        }

        private void flushStack() {
            if (edgeStack.isEmpty()) {
                return;
            }
            ArrayList<Integer> block = new ArrayList<>();
            while (!edgeStack.isEmpty()) {
                block.add(edgeStack.pop());
            }
            blocks.add(block);
        }

        private int other(int edge, int u) {
            return edges[edge].other(u);
        }

        @SuppressWarnings("unchecked")
        private static ArrayList<Integer>[] initAdjacency(int n) {
            ArrayList<Integer>[] out = (ArrayList<Integer>[]) new ArrayList<?>[n];
            for (int i = 0; i < n; i++) {
                out[i] = new ArrayList<>();
            }
            return out;
        }

        private static final class Edge {
            private final int u;
            private final int v;

            private Edge(int u, int v) {
                this.u = u;
                this.v = v;
            }

            private boolean isLoop() {
                return u == v;
            }

            private int other(int x) {
                return u == x ? v : u;
            }
        }
    }

    /**
     * Greedy series/parallel reducer that preserves terminal variables and
     * maintains disjoint edge masks for reduced components.
     */
    private static final class SeriesParallelReducer {
        private static final Comparator<VarCQ> VAR_ORDER = Comparator.comparing(VarCQ::getName);
        private final List<AtomCQ> atoms;
        private final java.util.function.Predicate<CPQ> componentFilter;

        SeriesParallelReducer(ConjunctiveQuery query, java.util.function.Predicate<CPQ> componentFilter) {
            this.atoms = query.edges();
            this.componentFilter = componentFilter;
        }

        List<Component> reduce(List<Integer> edgeIds, Set<VarCQ> terminals) {
            List<ReducedEdge> edges = new ArrayList<>(edgeIds.size());
            for (int edgeId : edgeIds) {
                AtomCQ atom = atoms.get(edgeId);
                VarCQ s = atom.getSource();
                VarCQ t = atom.getTarget();
                CPQ cpq = CPQ.label(atom.getLabel());
                if (s.equals(t)) {
                    cpq = CPQ.intersect(cpq, CPQ.id());
                }
                BitSet mask = new BitSet();
                mask.set(edgeId);
                edges.add(new ReducedEdge(s, t, cpq, mask, 1));
            }

            while (true) {
                if (reduceParallel(edges)) {
                    continue;
                }
                if (reduceSeries(edges, terminals)) {
                    continue;
                }
                break;
            }

            List<Component> out = new ArrayList<>();
            for (ReducedEdge edge : edges) {
                if (!edge.active) {
                    continue;
                }
                out.add(new Component(edge.s, edge.t, edge.diameter, edge.mask, edge.cpq, edge.cpq.toString()));
            }
            return out;
        }

        private boolean reduceParallel(List<ReducedEdge> edges) {
            Map<EndpointPair, List<ReducedEdge>> groups = new HashMap<>();
            for (ReducedEdge edge : edges) {
                if (!edge.active) {
                    continue;
                }
                EndpointPair key = EndpointPair.ordered(edge.s, edge.t);
                groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(edge);
            }

            List<EndpointPair> orderedPairs = new ArrayList<>(groups.keySet());
            orderedPairs.sort(Comparator.comparing((EndpointPair p) -> p.s.getName())
                    .thenComparing(p -> p.t.getName()));

            for (EndpointPair pair : orderedPairs) {
                List<ReducedEdge> group = groups.get(pair);
                if (group.size() < 2) {
                    continue;
                }
                VarCQ s = pair.s;
                VarCQ t = pair.t;
                CPQ merged = null;
                int diameter = 0;
                BitSet mask = new BitSet();
                for (ReducedEdge edge : group) {
                    CPQ part = oriented(edge, s, t);
                    merged = merged == null ? part : CPQ.intersect(merged, part);
                    diameter = Math.max(diameter, edge.diameter);
                    mask.or(edge.mask);
                }
                merged = ensureUnary(merged, s, t);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                for (ReducedEdge edge : group) {
                    edge.active = false;
                }
                edges.add(new ReducedEdge(s, t, merged, mask, diameter));
                return true;
            }
            return false;
        }

        private boolean reduceSeries(List<ReducedEdge> edges, Set<VarCQ> terminals) {
            Map<VarCQ, List<ReducedEdge>> incident = new HashMap<>();
            for (ReducedEdge edge : edges) {
                if (!edge.active) {
                    continue;
                }
                incident.computeIfAbsent(edge.s, ignored -> new ArrayList<>()).add(edge);
                if (!edge.s.equals(edge.t)) {
                    incident.computeIfAbsent(edge.t, ignored -> new ArrayList<>()).add(edge);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            for (VarCQ v : vertices) {
                if (terminals.contains(v)) {
                    continue;
                }
                List<ReducedEdge> list = incident.get(v);
                if (list.size() != 2) {
                    continue;
                }
                ReducedEdge left = list.get(0);
                ReducedEdge right = list.get(1);
                if (left == right || left.isLoop() || right.isLoop()) {
                    continue;
                }
                VarCQ a = left.other(v);
                VarCQ b = right.other(v);
                if (a == null || b == null) {
                    throw new IllegalStateException("Series reduction edge not incident to vertex");
                }
                if (a.equals(b)) {
                    continue;
                }

                CPQ first = oriented(left, a, v);
                CPQ second = oriented(right, v, b);
                CPQ merged = CPQ.concat(first, second);
                merged = ensureUnary(merged, a, b);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                BitSet mask = union(left.mask, right.mask);
                int diameter = left.diameter + right.diameter;

                left.active = false;
                right.active = false;
                edges.add(new ReducedEdge(a, b, merged, mask, diameter));
                return true;
            }
            return false;
        }

        private static CPQ oriented(ReducedEdge edge, VarCQ s, VarCQ t) {
            if (edge.s.equals(s) && edge.t.equals(t)) {
                return edge.cpq;
            }
            if (edge.s.equals(t) && edge.t.equals(s)) {
                return invert(edge.cpq);
            }
            throw new IllegalStateException("Edge endpoints do not match requested orientation");
        }

        private static CPQ ensureUnary(CPQ cpq, VarCQ s, VarCQ t) {
            if (s.equals(t)) {
                return CPQ.intersect(cpq, CPQ.id());
            }
            return cpq;
        }

        private static BitSet union(BitSet left, BitSet right) {
            BitSet out = (BitSet) left.clone();
            out.or(right);
            return out;
        }

        private static CPQ invert(CPQ cpq) {
            return invertTree(cpq.toAbstractSyntaxTree());
        }

        private static CPQ invertTree(QueryTree node) {
            return switch (node.getOperation()) {
                case IDENTITY -> CPQ.id();
                case EDGE -> {
                    Predicate label = node.getEdgeAtom().getLabel();
                    yield CPQ.label(label.getInverse());
                }
                case CONCATENATION -> {
                    int arity = node.getArity();
                    CPQ out = invertTree(node.getOperand(arity - 1));
                    for (int i = arity - 2; i >= 0; i--) {
                        out = CPQ.concat(out, invertTree(node.getOperand(i)));
                    }
                    yield out;
                }
                case INTERSECTION -> {
                    int arity = node.getArity();
                    CPQ out = invertTree(node.getOperand(0));
                    for (int i = 1; i < arity; i++) {
                        out = CPQ.intersect(out, invertTree(node.getOperand(i)));
                    }
                    yield out;
                }
                default -> throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
            };
        }
    }

    private static final class ReducedEdge {
        private final VarCQ s;
        private final VarCQ t;
        private final CPQ cpq;
        private final BitSet mask;
        private final int diameter;
        private boolean active;

        private ReducedEdge(VarCQ s, VarCQ t, CPQ cpq, BitSet mask, int diameter) {
            this.s = Objects.requireNonNull(s, "s");
            this.t = Objects.requireNonNull(t, "t");
            this.cpq = Objects.requireNonNull(cpq, "cpq");
            this.mask = (BitSet) Objects.requireNonNull(mask, "mask").clone();
            this.diameter = diameter;
            this.active = true;
        }

        private boolean isLoop() {
            return s.equals(t);
        }

        private VarCQ other(VarCQ v) {
            if (s.equals(v)) {
                return t;
            }
            if (t.equals(v)) {
                return s;
            }
            return null;
        }
    }

    private record EndpointPair(VarCQ s, VarCQ t) {
        private static EndpointPair ordered(VarCQ a, VarCQ b) {
            return a.getName().compareTo(b.getName()) <= 0
                    ? new EndpointPair(a, b)
                    : new EndpointPair(b, a);
        }
    }
}
