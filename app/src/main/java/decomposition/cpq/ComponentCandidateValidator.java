package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates whether a CPQ candidate actually matches the underlying component.
 */
final class ComponentCandidateValidator {

    private final List<Edge> edges;

    ComponentCandidateValidator(List<Edge> edges) {
        this.edges = edges;
    }

    KnownComponent ensureLoopAnchored(KnownComponent candidate) {
        if (!candidate.source().equals(candidate.target())) {
            return candidate;
        }
        try {
            QueryGraphCPQ graph = candidate.cpq().toQueryGraph();
            if (graph.isLoop()) {
                return candidate;
            }
        } catch (RuntimeException ex) {
            return candidate;
        }
        try {
            CPQ anchored = CPQ.parse("(" + candidate.cpq().toString() + " ∩ id)");
            String derivation = candidate.derivation() + " + anchored with id";
            return KnownComponentFactory.create(
                    anchored,
                    candidate.edges(),
                    candidate.source(),
                    candidate.target(),
                    derivation);
        } catch (RuntimeException ex) {
            return candidate;
        }
    }

    boolean matchesComponent(KnownComponent candidate) {
        BitSet edgeBits = candidate.edges();
        List<Edge> componentEdges = edgesFor(edgeBits);
        if (componentEdges.isEmpty()) {
            return false;
        }

        LinkedHashSet<String> vertices = new LinkedHashSet<>();
        for (Edge edge : componentEdges) {
            vertices.add(edge.source());
            vertices.add(edge.target());
        }
        if (!vertices.contains(candidate.source()) || !vertices.contains(candidate.target())) {
            return false;
        }

        CQ cqPattern;
        QueryGraphCQ cqGraph;
        UniqueGraph<VarCQ, AtomCQ> graph;
        try {
            cqPattern = candidate.cpq().toCQ();
            cqGraph = cqPattern.toQueryGraph();
            graph = cqGraph.toUniqueGraph();
        } catch (RuntimeException ex) {
            return false;
        }

        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = graph.getEdges();
        if (cpqEdges.size() != componentEdges.size()) {
            return false;
        }

        QueryGraphCPQ cpqGraph = candidate.cpq().toQueryGraph();
        boolean cpqEnforcesLoop = cpqGraph.isLoop();
        boolean candidateIsLoop = candidate.source().equals(candidate.target());
        if (cpqEnforcesLoop != candidateIsLoop) {
            return false;
        }
        String sourceVarName = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
        String targetVarName = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());

        Map<String, String> variableMapping = new HashMap<>();
        Set<String> usedNodes = new HashSet<>();
        variableMapping.put(sourceVarName, candidate.source());
        variableMapping.put(targetVarName, candidate.target());
        usedNodes.add(candidate.source());
        usedNodes.add(candidate.target());

        return matchEdges(0, cpqEdges, new ArrayList<>(componentEdges), variableMapping, usedNodes,
                sourceVarName, targetVarName, candidate.source(), candidate.target());
    }

    private List<Edge> edgesFor(BitSet bits) {
        List<Edge> selected = new ArrayList<>(bits.cardinality());
        for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
            selected.add(edges.get(idx));
        }
        return selected;
    }

    private boolean matchEdges(int index,
                               List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
                               List<Edge> remaining,
                               Map<String, String> variableMapping,
                               Set<String> usedNodes,
                               String sourceVarName,
                               String targetVarName,
                               String expectedSource,
                               String expectedTarget) {
        if (index == cpqEdges.size()) {
            String mappedSource = variableMapping.get(sourceVarName);
            String mappedTarget = variableMapping.get(targetVarName);
            return remaining.isEmpty()
                    && expectedSource.equals(mappedSource)
                    && expectedTarget.equals(mappedTarget);
        }

        GraphEdge<VarCQ, AtomCQ> cpqEdge = cpqEdges.get(index);
        AtomCQ atom = cpqEdge.getData();
        String label = atom.getLabel().getAlias();
        String cpqSrcName = cpqEdge.getSourceNode().getData().getName();
        String cpqTrgName = cpqEdge.getTargetNode().getData().getName();

        for (int i = 0; i < remaining.size(); i++) {
            Edge edge = remaining.get(i);
            if (!label.equals(edge.label())) {
                continue;
            }
            String componentSource = edge.source();
            String componentTarget = edge.target();

            String mappedSrc = variableMapping.get(cpqSrcName);
            String mappedTrg = variableMapping.get(cpqTrgName);

            if (mappedSrc != null && !mappedSrc.equals(componentSource)) {
                continue;
            }
            if (mappedTrg != null && !mappedTrg.equals(componentTarget)) {
                continue;
            }
            if (mappedSrc == null && usedNodes.contains(componentSource)) {
                continue;
            }
            if (mappedTrg == null && usedNodes.contains(componentTarget)) {
                continue;
            }

            boolean addedSrc = false;
            boolean addedTrg = false;
            if (mappedSrc == null) {
                variableMapping.put(cpqSrcName, componentSource);
                usedNodes.add(componentSource);
                addedSrc = true;
            }
            if (mappedTrg == null) {
                variableMapping.put(cpqTrgName, componentTarget);
                usedNodes.add(componentTarget);
                addedTrg = true;
            }

            Edge removed = remaining.remove(i);
            if (matchEdges(index + 1, cpqEdges, remaining, variableMapping, usedNodes,
                    sourceVarName, targetVarName, expectedSource, expectedTarget)) {
                return true;
            }

            remaining.add(i, removed);
            if (addedSrc) {
                variableMapping.remove(cpqSrcName);
                usedNodes.remove(componentSource);
            }
            if (addedTrg) {
                variableMapping.remove(cpqTrgName);
                usedNodes.remove(componentTarget);
            }
        }

        return false;
    }
}
