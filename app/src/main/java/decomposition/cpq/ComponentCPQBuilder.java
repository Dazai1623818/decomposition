package decomposition.cpq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;

/**
 * Builds CPQ expressions for connected components using gMark's CPQ model.
 *
 * Design principles:
 * - Every CPQ records the concrete traversal direction via its source → target
 * - Reverse directionality is expressed via inverse LABELS (r⁻) when needed
 * - Components may expose both forward and inverse orientations when the CQ permits it
 * - Matching is strictly directional
 * - Structural equivalence is defined solely by the covered edge bitset and oriented endpoints;
 *   internal CPQ syntax is ignored for deduplication
 */
public final class ComponentCPQBuilder {
    private final List<Edge> edges;
    private final Map<String, List<KnownComponent>> memo = new HashMap<>();

    public ComponentCPQBuilder(List<Edge> edges) {
        this.edges = List.copyOf(edges);
    }

    public List<Edge> allEdges() {
        return edges;
    }

    public List<KnownComponent> options(BitSet edgeBits) {
        return enumerate(edgeBits);
    }

    private List<KnownComponent> enumerate(BitSet edgeBits) {
        String signature = BitsetUtils.signature(edgeBits, edges.size());
        if (memo.containsKey(signature)) {
            return memo.get(signature);
        }

        Map<ComponentKey, KnownComponent> results = new LinkedHashMap<>();
        int cardinality = edgeBits.cardinality();

        if (cardinality == 0) {
            List<KnownComponent> emptyList = List.of();
            memo.put(signature, emptyList);
            return emptyList;
        }

        if (cardinality == 1) {
            int edgeIndex = edgeBits.nextSetBit(0);
            Edge edge = edges.get(edgeIndex);
            BitSet bitsCopy = BitsetUtils.copy(edgeBits);

            // 0) Plain forward atom: s --r--> t
            CPQ forwardCPQ = CPQ.parse(edge.label());
            KnownComponent forward = derived(
                forwardCPQ,
                bitsCopy,
                edge.source(),
                edge.target(),
                "Forward atom on label '" + edge.label() + "' (" + edge.source() + "→" + edge.target() + ")"
            );
            tryAdd(results, forward);

            // 0a) Inverse atom: t --r⁻--> s (only when endpoints differ)
            if (!edge.source().equals(edge.target())) {
                String inverseStr = edge.label() + "⁻";
                try {
                    CPQ inverseCPQ = CPQ.parse(inverseStr);
                    KnownComponent inverse = derived(
                        inverseCPQ,
                        bitsCopy,
                        edge.target(),
                        edge.source(),
                        "Inverse atom on label '" + edge.label() + "' (" + edge.target() + "→" + edge.source() + ")"
                    );
                    tryAdd(results, inverse);
                } catch (RuntimeException ex) {
                    // Skip if inverse parsing fails
                }

                // 0b) Backtrack at source: s --r--> t --r⁻--> s anchored with id
                String backtrackSourceStr = "((" + edge.label() + " ◦ " + edge.label() + "⁻) ∩ id)";
                try {
                    CPQ backtrackSource = CPQ.parse(backtrackSourceStr);
                    KnownComponent loopSource = derived(
                        backtrackSource,
                        bitsCopy,
                        edge.source(),
                        edge.source(),
                        "Backtrack loop via '" + edge.label() + "' at " + edge.source()
                    );
                    tryAdd(results, loopSource);
                } catch (RuntimeException ex) {
                    // Ignore unparsable backtrack variants
                }

                // 0c) Backtrack at target: t --r⁻--> s --r--> t anchored with id
                String backtrackTargetStr = "((" + edge.label() + "⁻ ◦ " + edge.label() + ") ∩ id)";
                try {
                    CPQ backtrackTarget = CPQ.parse(backtrackTargetStr);
                    KnownComponent loopTarget = derived(
                        backtrackTarget,
                        bitsCopy,
                        edge.target(),
                        edge.target(),
                        "Backtrack loop via '" + edge.label() + "' at " + edge.target()
                    );
                    tryAdd(results, loopTarget);
                } catch (RuntimeException ex) {
                    // Ignore unparsable backtrack variants
                }
            }

        } else {
            // unchanged: your existing composition logic
            List<Integer> indices = BitsetUtils.toIndexList(edgeBits);
            int combos = 1 << indices.size();
            for (int mask = 1; mask < combos - 1; mask++) {
                BitSet subsetA = new BitSet(edges.size());
                for (int i = 0; i < indices.size(); i++) {
                    if ((mask & (1 << i)) != 0) {
                        subsetA.set(indices.get(i));
                    }
                }

                BitSet subsetB = BitsetUtils.copy(edgeBits);
                subsetB.andNot(subsetA);

                List<KnownComponent> leftList = enumerate(subsetA);
                List<KnownComponent> rightList = enumerate(subsetB);
                if (leftList.isEmpty() || rightList.isEmpty()) {
                    continue;
                }

                for (KnownComponent left : leftList) {
                    for (KnownComponent right : rightList) {
                        // concatenation: shared middle
                        if (left.target().equals(right.source())) {
                            String concatStr = "(" + left.cpq().toString() + " ◦ " + right.cpq().toString() + ")";
                            CPQ concatCPQ = CPQ.parse(concatStr);
                            String derivation = "Concatenation: [" + left.cpqRule() + "] then [" + right.cpqRule()
                                    + "] via " + left.target();
                            KnownComponent combined = derived(
                                concatCPQ, BitsetUtils.copy(edgeBits), left.source(), right.target(), derivation
                            );
                            tryAdd(results, combined);
                        }
                        // conjunction: same endpoints
                        if (left.source().equals(right.source()) && left.target().equals(right.target())) {
                            String conjStr = "(" + left.cpq().toString() + " ∩ " + right.cpq().toString() + ")";
                            CPQ conjCPQ = CPQ.parse(conjStr);
                            String derivation = "Intersection: [" + left.cpqRule() + "] ∩ [" + right.cpqRule()
                                    + "] at " + left.source() + "→" + left.target();
                            KnownComponent combined = derived(
                                conjCPQ, BitsetUtils.copy(edgeBits), left.source(), left.target(), derivation
                            );
                            tryAdd(results, combined);
                        }
                    }
                }
            }

        }

        List<KnownComponent> finalList = List.copyOf(results.values());
        memo.put(signature, finalList);
        return finalList;
    }

    private KnownComponent derived(CPQ cpq, BitSet bits, String source, String target, String derivation) {
        return new KnownComponent(cpq, bits, source, target, derivation);
    }

    private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
        KnownComponent adjusted = ensureLoopAnchored(candidate);
        try {
            adjusted.cpq().toString();
        } catch (RuntimeException ex) {
            return;
        }
        if (!matchesComponent(adjusted)) {
            return;
        }
        ComponentKey key = adjusted.toKey(edges.size());
        results.putIfAbsent(key, adjusted);
    }

    private KnownComponent ensureLoopAnchored(KnownComponent candidate) {
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
            return new KnownComponent(anchored, candidate.edges(), candidate.source(), candidate.target(), derivation);
        } catch (RuntimeException ex) {
            return candidate;
        }
    }

    private boolean matchesComponent(KnownComponent candidate) {
        BitSet edgeBits = candidate.edges();
        List<Edge> componentEdges = edgesFor(edgeBits);
        if (componentEdges.isEmpty()) {
            return false;
        }

        LinkedHashSet<String> componentVertices = new LinkedHashSet<>();
        for (Edge edge : componentEdges) {
            componentVertices.add(edge.source());
            componentVertices.add(edge.target());
        }

        if (!componentVertices.contains(candidate.source()) || !componentVertices.contains(candidate.target())) {
            return false;
        }

        CQ cqPattern;
        QueryGraphCQ queryGraph;
        UniqueGraph<VarCQ, AtomCQ> graph;
        try {
            cqPattern = candidate.cpq().toCQ();
            queryGraph = cqPattern.toQueryGraph();
            graph = queryGraph.toUniqueGraph();
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
