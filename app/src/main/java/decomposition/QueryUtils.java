package decomposition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashSet;
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


    public static CQ buildCQFromEdges(Collection<Partitioning.Edge> edges) {
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

    public static Set<String> getVarsFromEdges(Collection<Partitioning.Edge> edges) {
        return edges.stream().flatMap(e -> Stream.of(e.source, e.target)).collect(Collectors.toSet());
    }

    public static CPQ constructCPQFromEdges(Collection<Partitioning.Edge> edges) {
        List<Partitioning.Edge> edgeList = new ArrayList<>(edges);
        if (edgeList.size() == 1) return new EdgeCPQ(edgeList.get(0).getPredicate());

        Map<String, Partitioning.Edge> outMap = new HashMap<>();
        Map<String, Partitioning.Edge> inMap = new HashMap<>();
        Set<String> nodes = new HashSet<>();

        for (Partitioning.Edge edge : edgeList) {
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

            if (sequence.size() == edgeList.size()) return new ConcatCPQ(sequence);
        }

        return new IntersectionCPQ(edgeList.stream()
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
            List<List<Set<Partitioning.Edge>>> filteredPartitions,
            Set<String> freeVars) {

        Map<Set<Partitioning.Edge>, ComponentInfo> known = new HashMap<>();

        if (filteredPartitions.isEmpty()) return known;

        List<Set<Partitioning.Edge>> firstPartition = filteredPartitions.get(0);

        for (Set<Partitioning.Edge> component : firstPartition) {
            Set<Partitioning.Edge> componentSet = new HashSet<>(component);
            Partitioning.Edge edge = component.iterator().next();
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

    public static List<CPQ> generateBacktrackingCPQs(Set<Partitioning.Edge> component,
                                                     Set<String> joinNodes) {
        if (component == null || component.isEmpty()) return List.of();

        Map<String, List<Partitioning.Edge>> adjacency = new HashMap<>();
        for (Partitioning.Edge edge : component) {
            adjacency.computeIfAbsent(edge.source, k -> new ArrayList<>()).add(edge);
            adjacency.computeIfAbsent(edge.target, k -> new ArrayList<>()).add(edge);
        }

        Set<String> roots = new LinkedHashSet<>();
        if (joinNodes != null && !joinNodes.isEmpty()) {
            roots.addAll(joinNodes);
        }

        // Prefer leaves to anchor linear paths when join nodes are absent
        for (Map.Entry<String, List<Partitioning.Edge>> entry : adjacency.entrySet()) {
            if (entry.getValue().size() == 1) {
                roots.add(entry.getKey());
            }
        }

        if (roots.isEmpty()) {
            roots.add(component.iterator().next().source);
        }

        Set<String> expressions = new LinkedHashSet<>();
        for (String root : roots) {
            List<Partitioning.Edge> neighbors = adjacency.getOrDefault(root, List.of());
            for (Partitioning.Edge edge : neighbors) {
                Set<Partitioning.Edge> visited = new HashSet<>();
                visited.add(edge);

                String neighbor = edge.source.equals(root) ? edge.target : edge.source;
                String forwardLabel = edge.source.equals(root)
                        ? edge.predicate.getAlias()
                        : edge.predicate.getInverse().getAlias();

                String subtreeExpr = buildBacktrackingSubtree(neighbor, root, adjacency, visited);
                StringBuilder builder = new StringBuilder();
                builder.append(forwardLabel);
                if (!subtreeExpr.isEmpty()) {
                    builder.append("◦(").append(subtreeExpr).append(")");
                }
                expressions.add(builder.toString());
            }
        }

        List<CPQ> cpqs = new ArrayList<>();
        for (String expr : expressions) {
            try {
                cpqs.add(CPQ.parse(expr));
            } catch (IllegalArgumentException ignored) {
                // skip expressions that fail to parse; they are not valid CPQs
            }
        }
        return cpqs;
    }

    private static String buildBacktrackingSubtree(String node,
                                                   String parent,
                                                   Map<String, List<Partitioning.Edge>> adjacency,
                                                   Set<Partitioning.Edge> visited) {
        List<String> branches = new ArrayList<>();
        for (Partitioning.Edge edge : adjacency.getOrDefault(node, List.of())) {
            if (visited.contains(edge)) continue;

            String neighbor = edge.source.equals(node) ? edge.target : edge.source;
            if (neighbor.equals(parent)) continue;

            visited.add(edge);

            String forwardLabel = edge.source.equals(node)
                    ? edge.predicate.getAlias()
                    : edge.predicate.getInverse().getAlias();
            String backwardLabel = edge.source.equals(node)
                    ? edge.predicate.getInverse().getAlias()
                    : edge.predicate.getAlias();

            String childExpr = buildBacktrackingSubtree(neighbor, node, adjacency, visited);

            StringBuilder branch = new StringBuilder();
            branch.append(forwardLabel);
            if (!childExpr.isEmpty()) {
                branch.append("◦(").append(childExpr).append(")");
            }
            branch.append("◦").append(backwardLabel);

            branches.add("(" + branch + ")∩id");
        }
        return String.join("◦", branches);
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





    public static boolean isIsomorphic(CPQ cpq, Collection<Partitioning.Edge> componentEdges, Set<String> allowedJoinNodes) {
        // check here when we have a matching of backtrack with something else
        UniqueGraph<VarCQ, AtomCQ> graph = cpq.toCQ().toQueryGraph().toUniqueGraph();
        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = graph.getEdges();

        Map<String, String> varToNode = new HashMap<>();
        Set<String> usedComponentNodes = new HashSet<>();
        List<Partitioning.Edge> componentEdgeList = new ArrayList<>(componentEdges);
        Set<Partitioning.Edge> matchedEdges = new HashSet<>();
        Set<Partitioning.Edge> forwardMatched = new HashSet<>();

        for (GraphEdge<VarCQ, AtomCQ> cpqEdge : cpqEdges) {
            String cpqSrc = cpqEdge.getSource().getName();
            String cpqTrg = cpqEdge.getTarget().getName();
            Predicate cpqPredicate = cpqEdge.getData().getLabel();
            String cpqLabel = cpqPredicate.getAlias();

            boolean matched = false;

            for (Partitioning.Edge componentEdge : componentEdgeList) {
                Predicate componentPredicate = componentEdge.predicate;
                String edgeLabel = componentPredicate.getAlias();

                boolean reverseMatch = false;
                if (!cpqLabel.equals(edgeLabel)) {
                    String inverseLabel = cpqPredicate.getInverse().getAlias();
                    if (!inverseLabel.equals(edgeLabel)) {
                        continue;
                    }
                    reverseMatch = true;
                }

                if (!reverseMatch && forwardMatched.contains(componentEdge)) continue;

                String edgeSrc = reverseMatch ? componentEdge.target : componentEdge.source;
                String edgeTrg = reverseMatch ? componentEdge.source : componentEdge.target;


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
                    if (!reverseMatch) {
                        forwardMatched.add(componentEdge);
                    }
                    matchedEdges.add(componentEdge);
                    matched = true;
                    break;
                }
            }

            if (!matched) return false;
        }

        if (matchedEdges.size() != componentEdgeList.size()) return false;

        // Check allowedJoinNodes constraints
        if (allowedJoinNodes.size() == 1) {
            String allowedNode = allowedJoinNodes.iterator().next();
            if (!varToNode.containsValue(allowedNode)) {
                return false; // ensure the single join node appears in the mapping
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



    public static Set<String> getNodesFromEdges(Collection<Edge> edges) {
        Set<String> nodes = new HashSet<>();
        for (Edge edge : edges) {
            nodes.add(edge.source);
            nodes.add(edge.target);
        }
        return nodes;
    }


}
