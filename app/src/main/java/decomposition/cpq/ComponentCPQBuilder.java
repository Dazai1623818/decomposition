package decomposition.cpq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;

/**
 * Builds CPQ expressions for connected components using gMark's CPQ model.
 *
 * Design principles:
 * - All CPQs are forward-directional (source → target)
 * - Reverse directionality is expressed via inverse LABELS (r⁻), not reversed CPQs
 * - Components never swap source/target endpoints
 * - Matching is strictly directional
 */
final class ComponentCPQBuilder {
    private final List<Edge> edges;
    private final Map<String, List<KnownComponent>> memo = new HashMap<>();

    ComponentCPQBuilder(List<Edge> edges) {
        this.edges = List.copyOf(edges);
    }

    int totalEdges() {
        return edges.size();
    }

    Optional<KnownComponent> build(BitSet edgeBits) {
        List<KnownComponent> candidates = enumerate(edgeBits);
        return candidates.isEmpty() ? Optional.empty() : Optional.of(candidates.get(0));
    }

    List<KnownComponent> enumerateAll(BitSet edgeBits) {
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

            // 1) Enrich single-edge case with backtrack/selfloop/multi forms.
            addSingleEdgeBacktrackVariants(results, edge, bitsCopy);

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

            // Multi-edge backtrack variants: extend backtrack patterns when cardinality > 1
            addMultiEdgeBacktrackVariants(results, edgeBits);
        }

        // Post-processing: add anchored variants for same-node components if not already anchored.
        // REMOVED: Inverse CPQ generation - reverse direction is only supported via inverse labels (r⁻),
        // not by reversing entire CPQs and swapping endpoints.
        List<KnownComponent> snapshot = new ArrayList<>(results.values());
        List<KnownComponent> additional = new ArrayList<>();
        for (KnownComponent candidate : snapshot) {
            // Only add ... ∩ id if it's a same-node component AND not already anchored
            if (candidate.source().equals(candidate.target()) && !looksAnchored(candidate.cpq())) {
                String anchoredStr = candidate.cpq().toString() + " ∩ id";
                try {
                    CPQ anchoredCPQ = CPQ.parse(anchoredStr);
                    String derivation = "Anchor with id: [" + candidate.cpqRule() + "] ∩ id at "
                            + candidate.source();
                    KnownComponent anchoredComponent = derived(
                        anchoredCPQ, candidate.edges(), candidate.source(), candidate.target(), derivation
                    );
                    additional.add(anchoredComponent);
                } catch (RuntimeException ex) {
                    // Skip if anchoring fails
                }
            }
        }
        for (KnownComponent extra : additional) {
            tryAdd(results, extra);
        }

        List<KnownComponent> finalList = List.copyOf(results.values());
        memo.put(signature, finalList);
        return finalList;
    }

    /**
     * Adds backtrack variants using inverse labels (r⁻) within CPQs for SINGLE edges only.
     * All components maintain forward direction (source → target).
     *
     * Generates only:
     *  - single backtracks: (r◦r⁻)∩id anchored at source, (r⁻◦r)∩id anchored at target
     *  - selfloop: if source==target also add (r)∩id
     *
     * Multi-edge backtracks (double, multi) are handled separately during composition
     * when cardinality > 1, as they represent extending the backtrack case.
     *
     * Note: r⁻ is an inverse LABEL, not a reversed CPQ. Endpoints are never swapped.
     */
    private void addSingleEdgeBacktrackVariants(Map<ComponentKey, KnownComponent> results, Edge e, BitSet bits) {
        String s = e.source();
        String t = e.target();
        String r = e.label();

        // --- single backtracks (anchored) ---
        // (r◦r⁻) ∩ id at source
        String singleSStr = "(" + r + " ◦ (" + r + ")⁻) ∩ id";
        try {
            CPQ singleSCPQ = CPQ.parse(singleSStr);
            String derivation = "Single backtrack anchored at " + s + " via " + singleSStr;
            tryAdd(results, derived(singleSCPQ, bits, s, s, derivation));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // (r⁻◦r) ∩ id at target
        String singleTStr = "((" + r + ")⁻ ◦ " + r + ") ∩ id";
        try {
            CPQ singleTCPQ = CPQ.parse(singleTStr);
            String derivation = "Single backtrack anchored at " + t + " via " + singleTStr;
            tryAdd(results, derived(singleTCPQ, bits, t, t, derivation));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // --- self-loop convenience: if s==t, also expose r∩id explicitly ---
        if (s.equals(t)) {
            String rAnchoredStr = r + " ∩ id";
            try {
                CPQ rAnchoredCPQ = CPQ.parse(rAnchoredStr);
                String derivation = "Self-loop anchor at " + s + " exposing " + rAnchoredStr;
                tryAdd(results, derived(rAnchoredCPQ, bits, s, s, derivation));
            } catch (RuntimeException ex) {
                // Skip if parsing fails
            }
        }
    }

    /**
     * Adds multi-edge backtrack variants when cardinality > 1.
     * These patterns extend the single backtrack case by wrapping existing components.
     *
     * Generates patterns like:
     *  - (r◦q◦r⁻)∩id where q is an anchored component and r is from decomposition
     *  - Double backtracks and higher-order patterns via composition
     *
     * This is separate from concat and conjunction as it represents extending
     * the backtrack case with multiple edge traversals.
     */
    private void addMultiEdgeBacktrackVariants(Map<ComponentKey, KnownComponent> results, BitSet edgeBits) {
        List<KnownComponent> snapshot = new ArrayList<>(results.values());

        for (KnownComponent component : snapshot) {
            // For each component that uses these edges, try to find single-edge subsets
            // that could wrap around it for backtracking patterns
            List<Integer> indices = BitsetUtils.toIndexList(edgeBits);

            for (int edgeIndex : indices) {
                Edge edge = edges.get(edgeIndex);
                String r = edge.label();
                String s = edge.source();
                String t = edge.target();

                // Pattern: r ◦ q ◦ r⁻ anchored at source
                // where q is anchored at target and uses remaining edges
                if (component.source().equals(t) && component.target().equals(t)) {
                    String backtrackStr = "(" + r + " ◦ " + component.cpq().toString() + " ◦ (" + r + ")⁻) ∩ id";
                    try {
                        CPQ backtrackCPQ = CPQ.parse(backtrackStr);
                        String derivation = "Extended backtrack at " + s + " wrapping [" + component.cpqRule() + "]";
                        tryAdd(results, derived(backtrackCPQ, edgeBits, s, s, derivation));
                    } catch (RuntimeException ex) {
                        // Skip if parsing fails
                    }
                }

                // Symmetric pattern: r⁻ ◦ q ◦ r anchored at target
                // where q is anchored at source and uses remaining edges
                if (component.source().equals(s) && component.target().equals(s)) {
                    String backtrackStr = "((" + r + ")⁻ ◦ " + component.cpq().toString() + " ◦ " + r + ") ∩ id";
                    try {
                        CPQ backtrackCPQ = CPQ.parse(backtrackStr);
                        String derivation = "Extended backtrack at " + t + " wrapping [" + component.cpqRule() + "]";
                        tryAdd(results, derived(backtrackCPQ, edgeBits, t, t, derivation));
                    } catch (RuntimeException ex) {
                        // Skip if parsing fails
                    }
                }
            }
        }
    }

    /**
     * Lightweight detector to avoid ((...) ∩ id) ∩ id duplicates.
     * Checks if the CPQ is already anchored with identity based on its string representation.
     */
    private boolean looksAnchored(CPQ cpq) {
        String str = cpq.toString().replace(" ", "");
        return str.endsWith("∩id") || str.endsWith("∩id)") || str.contains(")∩id");
    }

    private KnownComponent derived(CPQ cpq, BitSet bits, String source, String target, String derivation) {
        return new KnownComponent(cpq, bits, source, target, derivation);
    }

    private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
        // Validate that the CPQ is valid (it should be since we built it via CPQ.parse)
        try {
            candidate.cpq().toString(); // Just ensure it's valid
        } catch (RuntimeException ex) {
            // Skip invalid CPQs
            return;
        }
        ComponentKey key = candidate.toKey(edges.size());
        KnownComponent existing = results.get(key);
        if (existing == null) {
            results.put(key, candidate);
            return;
        }
        if (shouldReplace(existing, candidate)) {
            results.put(key, candidate);
        }
    }

    private boolean shouldReplace(KnownComponent existing, KnownComponent candidate) {
        boolean existingAnchored = looksAnchored(existing.cpq());
        boolean candidateAnchored = looksAnchored(candidate.cpq());
        if (candidateAnchored && !existingAnchored) {
            return true;
        }
        if (candidateAnchored == existingAnchored) {
            // Prefer more descriptive derivation (longer CPQ string) as a heuristic for richer structure.
            int existingLength = existing.cpqRule().length();
            int candidateLength = candidate.cpqRule().length();
            if (candidateLength > existingLength) {
                return true;
            }
        }
        return false;
    }
}
