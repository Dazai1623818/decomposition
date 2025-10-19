package decomposition.cpq;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            KnownComponent forward = new KnownComponent(
                forwardCPQ,
                bitsCopy,
                edge.source(),
                edge.target()
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
                            KnownComponent combined = new KnownComponent(
                                concatCPQ, BitsetUtils.copy(edgeBits), left.source(), right.target()
                            );
                            tryAdd(results, combined);
                        }
                        // conjunction: same endpoints
                        if (left.source().equals(right.source()) && left.target().equals(right.target())) {
                            String conjStr = "(" + left.cpq().toString() + " ∩ " + right.cpq().toString() + ")";
                            CPQ conjCPQ = CPQ.parse(conjStr);
                            KnownComponent combined = new KnownComponent(
                                conjCPQ, BitsetUtils.copy(edgeBits), left.source(), left.target()
                            );
                            tryAdd(results, combined);
                        }
                    }
                }
            }
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
                    KnownComponent anchoredComponent = new KnownComponent(
                        anchoredCPQ, candidate.edges(), candidate.source(), candidate.target()
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
     * Adds backtrack variants using inverse labels (r⁻) within CPQs.
     * All components maintain forward direction (source → target).
     *
     * Generates:
     *  - single backtracks: (r◦r⁻)∩id anchored at source, (r⁻◦r)∩id anchored at target
     *  - double backtracks: (r◦((r◦r⁻)∩id)◦r⁻)∩id at source and symmetric at target
     *  - multi backtracks:  (r◦q◦r⁻)∩id at source for any q anchored at target (and symmetric)
     *  - selfloop: if source==target also add (r)∩id
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
            tryAdd(results, new KnownComponent(singleSCPQ, bits, s, s));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // (r⁻◦r) ∩ id at target
        String singleTStr = "((" + r + ")⁻ ◦ " + r + ") ∩ id";
        try {
            CPQ singleTCPQ = CPQ.parse(singleTStr);
            tryAdd(results, new KnownComponent(singleTCPQ, bits, t, t));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // --- double backtracks (anchored) ---
        // (r◦((r◦r⁻)∩id)◦r⁻) ∩ id at source
        String innerS = "(" + r + " ◦ (" + r + ")⁻) ∩ id";
        String dblSStr = "(" + r + " ◦ (" + innerS + ") ◦ (" + r + ")⁻) ∩ id";
        try {
            CPQ dblSCPQ = CPQ.parse(dblSStr);
            tryAdd(results, new KnownComponent(dblSCPQ, bits, s, s));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // (r⁻◦((r⁻◦r)∩id)◦r) ∩ id at target
        String innerT = "((" + r + ")⁻ ◦ " + r + ") ∩ id";
        String dblTStr = "((" + r + ")⁻ ◦ (" + innerT + ") ◦ " + r + ") ∩ id";
        try {
            CPQ dblTCPQ = CPQ.parse(dblTStr);
            tryAdd(results, new KnownComponent(dblTCPQ, bits, t, t));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // --- self-loop convenience: if s==t, also expose r∩id explicitly ---
        if (s.equals(t)) {
            String rAnchoredStr = r + " ∩ id";
            try {
                CPQ rAnchoredCPQ = CPQ.parse(rAnchoredStr);
                tryAdd(results, new KnownComponent(rAnchoredCPQ, bits, s, s));
            } catch (RuntimeException ex) {
                // Skip if parsing fails
            }
        }

        // --- multi backtracks: (r ◦ q ◦ r⁻) ∩ id (and symmetric) ---
        // Snapshot BEFORE adding new multis to avoid unbounded growth.
        List<KnownComponent> pre = new ArrayList<>(results.values());

        // q anchored at t → produce @s
        for (KnownComponent q : pre) {
            if (q.source().equals(t) && q.target().equals(t)) {
                String multiAtSStr = "(" + r + " ◦ " + q.cpq().toString() + " ◦ (" + r + ")⁻) ∩ id";
                try {
                    CPQ multiAtSCPQ = CPQ.parse(multiAtSStr);
                    tryAdd(results, new KnownComponent(multiAtSCPQ, bits, s, s));
                } catch (RuntimeException ex) {
                    // Skip if parsing fails
                }
            }
        }

        // q' anchored at s → produce @t (symmetric direction)
        for (KnownComponent q : pre) {
            if (q.source().equals(s) && q.target().equals(s)) {
                String multiAtTStr = "((" + r + ")⁻ ◦ " + q.cpq().toString() + " ◦ " + r + ") ∩ id";
                try {
                    CPQ multiAtTCPQ = CPQ.parse(multiAtTStr);
                    tryAdd(results, new KnownComponent(multiAtTCPQ, bits, t, t));
                } catch (RuntimeException ex) {
                    // Skip if parsing fails
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

    private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
        // Validate that the CPQ is valid (it should be since we built it via CPQ.parse)
        try {
            candidate.cpq().toString(); // Just ensure it's valid
        } catch (RuntimeException ex) {
            // Skip invalid CPQs
            return;
        }
        ComponentKey key = candidate.toKey(edges.size());
        results.putIfAbsent(key, candidate);
    }
}
