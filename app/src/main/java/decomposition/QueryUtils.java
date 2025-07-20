package decomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Iterator;

import dev.roanh.gmark.util.graph.GraphPanel;
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

    public static class ComponentInfo {
        public final CQ cq;
        public final List<CPQ> cpqs;
        public final Set<String> variables;
        public final Set<String> joinNodes;
        public final boolean isKnown;

        public ComponentInfo(CQ cq, List<CPQ> cpqs, Set<String> variables, Set<String> joinNodes, boolean isKnown) {
            this.cq = cq;
            this.cpqs = cpqs;
            this.variables = variables;
            this.joinNodes = joinNodes;
            this.isKnown = isKnown;
        }

        public ComponentInfo markKnownWithCPQ(CPQ newCpq) {
            List<CPQ> updated = new ArrayList<>(cpqs);
            updated.add(newCpq);
            return new ComponentInfo(cq, updated, variables, joinNodes, true);
        }
    }


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

            if (sequence.size() == edges.size()) return new ConcatCPQ(sequence);
        }

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

    public static Map<Set<Partitioning.Edge>, ComponentInfo> initializeKnownComponents(
            List<List<List<Partitioning.Edge>>> filteredPartitions,
            Set<String> freeVars) {

        Map<Set<Partitioning.Edge>, ComponentInfo> known = new HashMap<>();

        if (filteredPartitions.isEmpty()) return known;

        List<List<Partitioning.Edge>> firstPartition = filteredPartitions.get(0);

        for (List<Partitioning.Edge> component : firstPartition) {
            Set<Partitioning.Edge> componentSet = new HashSet<>(component);
            Partitioning.Edge edge = component.get(0);
            Predicate p = edge.getPredicate();
            Predicate pInv = p.getInverse();

            // CPQ for forward edge
            CPQ forward = CPQ.label(p);
            // CPQ for inverse edge
            CPQ inverse = CPQ.label(pInv);

            List<CPQ> cpqs = new ArrayList<>();
            
            // Check for loop edge
            if (edge.source.equals(edge.target)) {
                // For loops, intersect with identity
                cpqs.add(CPQ.intersect(forward, CPQ.id()));
                cpqs.add(CPQ.intersect(inverse, CPQ.id()));
            } else {
                cpqs.add(forward);
                cpqs.add(inverse);
            }

            // backtrack edge
            cpqs.add(CPQ.intersect(CPQ.concat(forward, inverse), CPQ.id()));
            cpqs.add(CPQ.intersect(CPQ.concat(inverse, forward), CPQ.id()));
            // GraphPanel.show(CPQ.intersect(CPQ.concat(forward, inverse), CPQ.id()));
            // GraphPanel.show(CPQ.intersect(CPQ.concat(inverse, forward), CPQ.id()));

            // Convert first CPQ (forward) to CQ just for variable and atom extraction
            CQ subCq = cpqs.get(0).toCQ();  // Any one is fine for structure
            Set<String> vars = getVarsFromEdges(component);
            Set<String> joinNodes = Partitioning.getJoinNodesPerComponent(List.of(component), freeVars).get(0);

            known.put(componentSet, new ComponentInfo(subCq, cpqs, vars, joinNodes, true));
        }

        return known;
    }



    public static void printEdgesFromCPQ(CPQ cpq) {
        System.out.println("    CPQ Expression: " + cpq);

        CQ cq = cpq.toCQ();
        UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
        List<GraphEdge<VarCQ, AtomCQ>> edges = graph.getEdges();

        for (GraphEdge<VarCQ, AtomCQ> edge : edges) {
            String src = edge.getSource().getName();
            String trg = edge.getTarget().getName();
            Predicate labelPred = edge.getData().getLabel();

            String label = labelPred.getAlias();
            if (labelPred.isInverse() && !label.endsWith("⁻")) {
                label += "⁻";
            }
            System.out.printf("      %s --%s--> %s\n", src, label, trg);
        }
    }





    public static boolean isInjectiveHomomorphic(CPQ cpq, List<Partitioning.Edge> componentEdges, Set<String> allowedJoinNodes) {
        UniqueGraph<VarCQ, AtomCQ> graph = cpq.toCQ().toQueryGraph().toUniqueGraph();
        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = graph.getEdges();

        Map<String, String> varToNode = new HashMap<>();
        Set<String> usedComponentNodes = new HashSet<>();
        List<Partitioning.Edge> unmatchedEdges = new ArrayList<>(componentEdges);

        for (GraphEdge<VarCQ, AtomCQ> cpqEdge : cpqEdges) {
            String cpqSrc = cpqEdge.getSource().getName();
            String cpqTrg = cpqEdge.getTarget().getName();
            String cpqLabel = cpqEdge.getData().getLabel().getAlias();

            boolean matched = false;

            for (Partitioning.Edge componentEdge : new ArrayList<>(unmatchedEdges)) {
                String edgeSrc = componentEdge.source;
                String edgeTrg = componentEdge.target;
                String edgeLabel = componentEdge.predicate.getAlias();

                if (!cpqLabel.equals(edgeLabel)) continue;

                // Check for consistency
                String mappedSrc = varToNode.get(cpqSrc);
                String mappedTrg = varToNode.get(cpqTrg);

                boolean srcCompatible = (mappedSrc == null || mappedSrc.equals(edgeSrc));
                boolean trgCompatible = (mappedTrg == null || mappedTrg.equals(edgeTrg));

                // Check injectivity
                boolean srcInjective = (mappedSrc != null && mappedSrc.equals(edgeSrc)) || !usedComponentNodes.contains(edgeSrc);
                boolean trgInjective = (mappedTrg != null && mappedTrg.equals(edgeTrg)) || !usedComponentNodes.contains(edgeTrg);

                if (srcCompatible && trgCompatible && srcInjective && trgInjective) {
                    varToNode.put(cpqSrc, edgeSrc);
                    varToNode.put(cpqTrg, edgeTrg);
                    usedComponentNodes.add(edgeSrc);
                    usedComponentNodes.add(edgeTrg);
                    unmatchedEdges.remove(componentEdge);
                    matched = true;
                    break;
                }
            }

            if (!matched) return false;
        }

        // Check allowedJoinNodes constraints
        if (allowedJoinNodes.size() == 1) {
            String allowedNode = allowedJoinNodes.iterator().next();
            String mappedNode = varToNode.get("src,trg");
            if (mappedNode == null || !mappedNode.equals(allowedNode)) {
                return false; // Return false if "src,trg" does not map to the single allowed join node
            }
        } else if (allowedJoinNodes.size() == 2) {
            Iterator<String> iterator = allowedJoinNodes.iterator();
            String node1 = iterator.next();
            String node2 = iterator.next();
            String srcMapped = varToNode.get("src");
            String trgMapped = varToNode.get("trg");

            // Check if src and trg map to the two distinct allowed nodes
            if (srcMapped == null || trgMapped == null) {
                return false; // Return false if either src or trg is not mapped
            }
            if (!((srcMapped.equals(node1) && trgMapped.equals(node2)) || 
                (srcMapped.equals(node2) && trgMapped.equals(node1)))) {
                return false; // Return false if src and trg don't map to the two nodes distinctly
            }
        }

        // System.out.println("Final variable-to-node mapping:");
        // for (Map.Entry<String, String> entry : varToNode.entrySet()) {
        //     if (entry.getKey().equals("src,trg") && allowedJoinNodes.size() == 1 && 
        //         entry.getValue().equals(allowedJoinNodes.iterator().next())) {
        //         System.out.println("  " + entry.getKey() + " -> " + entry.getValue());
        //     } else if (entry.getKey().equals("src") || entry.getKey().equals("trg")) {
        //         System.out.println("  " + entry.getKey() + " -> " + entry.getValue());
        //     }
        // }

        return true;
    }



    public static Set<String> getNodesFromEdges(List<Edge> edges) {
        Set<String> nodes = new HashSet<>();
        for (Edge edge : edges) {
            nodes.add(edge.source);
            nodes.add(edge.target);
        }
        return nodes;
    }


}