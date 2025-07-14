package decomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import decomposition.Partitioning.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.ConcatCPQ;
import dev.roanh.gmark.lang.cpq.EdgeCPQ;
import dev.roanh.gmark.lang.cpq.IntersectionCPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;


public class QueryUtils {

    // ---------------------------
    // ComponentInfo Data Class
    // ---------------------------

    public static class ComponentInfo {
        public final CQ cq;
        public final CPQ cpq;
        public final Set<String> variables;
        public final boolean isKnown;

        public ComponentInfo(CQ cq, CPQ cpq, Set<String> variables, boolean isKnown) {
            this.cq = cq;
            this.cpq = cpq;
            this.variables = variables;
            this.isKnown = isKnown;
        }

        public ComponentInfo markKnown() {
            return new ComponentInfo(cq, cpq, variables, true);
        }
        public ComponentInfo markKnownWithCPQ(CPQ newCpq) {
            return new ComponentInfo(cq, newCpq, variables, true);
        }

    }

    // ---------------------------
    // CQ Utilities
    // ---------------------------

    public static CQ buildCQFromEdges(List<Partitioning.Edge> edges) {
        CQ cq = CQ.empty();
        Map<String, VarCQ> varMap = new HashMap<>();
        int predicateId = 1;
        for (Partitioning.Edge edge : edges) {
            VarCQ u = varMap.computeIfAbsent(edge.source, cq::addBoundVariable);
            VarCQ v = varMap.computeIfAbsent(edge.target, cq::addBoundVariable);
            cq.addAtom(u, new Predicate(predicateId++, edge.label()), v);
        }
        return cq;
    }

    public static void printCQAtoms(CQ cq) {
        cq.toQueryGraph().toUniqueGraph().getEdges().stream()
                .map(e -> e.getData())
                .forEach(atom -> System.out.println(atom.getSource().getName() + " --"
                        + atom.getLabel().getAlias() + "--> "
                        + atom.getTarget().getName()));
    }

    public static List<Partitioning.Edge> extractEdgesFromCQ(CQ cq) {
        return cq.toQueryGraph().toUniqueGraph().getEdges().stream()
                .map(e -> e.getData())
                .map(atom -> new Partitioning.Edge(atom.getSource().getName(),
                                                   atom.getTarget().getName(),
                                                   atom.getLabel()))
                .collect(Collectors.toList());
    }

    public static Set<String> getVarsFromEdges(List<Partitioning.Edge> edges) {
        return edges.stream().flatMap(e -> Stream.of(e.source, e.target)).collect(Collectors.toSet());
    }

    // ---------------------------
    // CPQ Utilities
    // ---------------------------

    public static CPQ constructCPQFromEdges(List<Partitioning.Edge> edges) {
        if (edges.size() == 1) return new EdgeCPQ(edges.get(0).getPredicate());

        Map<String, Partitioning.Edge> outMap = new HashMap<>();
        Map<String, Partitioning.Edge> inMap = new HashMap<>();
        Set<String> nodes = new HashSet<>();

        for (Partitioning.Edge edge : edges) {
            outMap.put(edge.source, edge);
            inMap.put(edge.target, edge);
            nodes.add(edge.source);
            nodes.add(edge.target);
        }

        // If there's a valid start node (path-like), build ConcatCPQ
        String start = nodes.stream().filter(n -> !inMap.containsKey(n)).findFirst().orElse(null);
        if (start != null) {
            List<CPQ> sequence = new ArrayList<>();
            String curr = start;
            Set<String> visited = new HashSet<>();

            while (outMap.containsKey(curr) && !visited.contains(curr)) {
                visited.add(curr);
                Partitioning.Edge e = outMap.get(curr);
                sequence.add(new EdgeCPQ(e.getPredicate()));
                curr = e.target;
            }

            // Only use ConcatCPQ if full coverage and no cycle
            if (sequence.size() == edges.size()) return new ConcatCPQ(sequence);
        }

        // Fallback: Intersection over all edges
        return new IntersectionCPQ(edges.stream()
                .map(e -> new EdgeCPQ(e.getPredicate()))
                .collect(Collectors.toList()));
    }


    public static boolean isEquivalent(CPQ c1, CPQ c2) {
        boolean equivalent = c1.toString().equals(c2.toString());
        return equivalent;
    }

    public static Set<String> extractFreeVariableNames(CQ cq) {
        return cq.getFreeVariables().stream()
                .map(VarCQ::getName)
                .collect(Collectors.toSet());
    }

    public static Map<List<Partitioning.Edge>, ComponentInfo> initializeKnownComponents(List<List<List<Partitioning.Edge>>> filteredPartitions) {
        Map<List<Partitioning.Edge>, ComponentInfo> known = new HashMap<>();
        if (filteredPartitions.isEmpty()) return known;

        List<List<Partitioning.Edge>> firstPartition = filteredPartitions.get(0);
        for (List<Partitioning.Edge> component : firstPartition) {
            Predicate p = component.get(0).getPredicate();
            CPQ cpq = new EdgeCPQ(p);
            CQ subCq = buildCQFromEdges(component);
            Set<String> vars = getVarsFromEdges(component);
            known.put(component, new ComponentInfo(subCq, cpq, vars, true));

            System.out.println("Initial known CPQ: " + p.getAlias());
        }
        return known;
    }

    public static void printEdgesFromCPQ(CPQ cpq) {
        // Convert CPQ to CQ
        CQ cq = cpq.toCQ();

        // Convert CQ to QueryGraph
        UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();

        // Extract and print edges
        List<GraphEdge<VarCQ, AtomCQ>> edges = graph.getEdges();
        System.out.println("    Edges:");
        for (GraphEdge<VarCQ, AtomCQ> edge : edges) {
            System.out.println("      " + edge.getSource() + " --" + edge.getData().getLabel().getAlias() + "--> " + edge.getTarget());
        }
    }

    public static boolean Matches(CPQ cpq, List<Edge> componentEdges) {
        boolean result = isInjectiveHomomorphic(cpq, componentEdges);

        System.out.println("Matching CPQ against component:");
        printEdgesFromCPQ(cpq);
        componentEdges.forEach(edge -> System.out.println("  Component Edge: " + edge));
        System.out.println("  Match result: " + result);

        return result;
    }



    public static boolean isInjectiveHomomorphic(CPQ cpq, List<Edge> componentEdges) {
        // Convert CPQ to graph of atoms
        UniqueGraph<VarCQ, AtomCQ> graph = cpq.toCQ().toQueryGraph().toUniqueGraph();
        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = graph.getEdges();

        Map<String, String> varToNodeMap = new HashMap<>(); // CPQ var name → component node name
        Map<String, String> assignedNodeToVar = new HashMap<>(); // Enforce injectivity

        for (GraphEdge<VarCQ, AtomCQ> cpqEdge : cpqEdges) {
            String cpqSrc = cpqEdge.getSource().getName();
            String cpqTrg = cpqEdge.getTarget().getName();
            String label = cpqEdge.getData().getLabel().getAlias();

            boolean foundMatch = false;

            for (Edge compEdge : componentEdges) {
                if (!compEdge.label().equals(label)) continue;

                String compSrc = compEdge.source;
                String compTrg = compEdge.target;

                // Check existing variable bindings
                if (varToNodeMap.containsKey(cpqSrc) && !varToNodeMap.get(cpqSrc).equals(compSrc)) continue;
                if (varToNodeMap.containsKey(cpqTrg) && !varToNodeMap.get(cpqTrg).equals(compTrg)) continue;

                // Enforce injectivity: no two vars map to same node
                if (!varToNodeMap.containsKey(cpqSrc) && assignedNodeToVar.containsKey(compSrc)) continue;
                if (!varToNodeMap.containsKey(cpqTrg) && assignedNodeToVar.containsKey(compTrg)) continue;

                // Assign new bindings
                varToNodeMap.put(cpqSrc, compSrc);
                assignedNodeToVar.put(compSrc, cpqSrc);

                varToNodeMap.put(cpqTrg, compTrg);
                assignedNodeToVar.put(compTrg, cpqTrg);

                foundMatch = true;
                break;
            }

            if (!foundMatch) {
                return false; // Failed to match this CPQ edge
            }
        }

        return true;
    }

    

}
