package decomposition.cpq;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;

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
    private final Map<MemoKey, List<KnownComponent>> memo = new HashMap<>();
    private final ComponentCandidateValidator candidateValidator;

    public ComponentCPQBuilder(List<Edge> edges) {
        this.edges = List.copyOf(edges);
        this.candidateValidator = new ComponentCandidateValidator(this.edges);
    }

    public List<Edge> allEdges() {
        return edges;
    }

    public List<KnownComponent> options(BitSet edgeBits) {
        return options(edgeBits, Set.of());
    }

    public List<KnownComponent> options(BitSet edgeBits, Set<String> joinNodes) {
        Objects.requireNonNull(edgeBits, "edgeBits");
        Set<String> normalizedJoinNodes = joinNodes == null ? Set.of() : joinNodes;
        return enumerate(edgeBits, normalizedJoinNodes);
    }

    private List<KnownComponent> enumerate(BitSet edgeBits, Set<String> joinNodes) {
        String signature = BitsetUtils.signature(edgeBits, edges.size());
        Set<String> localJoinNodes = collectLocalJoinNodes(edgeBits, joinNodes);
        JoinMode joinMode = JoinMode.fromCount(localJoinNodes.size());
        MemoKey key = new MemoKey(signature, joinMode);
        if (memo.containsKey(key)) {
            return memo.get(key);
        }

        Map<ComponentKey, KnownComponent> results = new LinkedHashMap<>();
        int cardinality = edgeBits.cardinality();

        if (cardinality == 0) {
            List<KnownComponent> emptyList = List.of();
            memo.put(key, emptyList);
            return emptyList;
        }

        if (cardinality == 1) {
            int edgeIndex = edgeBits.nextSetBit(0);
            Edge edge = edges.get(edgeIndex);
            for (KnownComponent option : SingleEdgeOptionFactory.build(edge, edgeBits)) {
                tryAdd(results, option);
            }
        } else {
            if (joinMode.allowBacktracks()) {
                for (KnownComponent option : LoopBacktrackBuilder.build(edges, edgeBits, localJoinNodes)) {
                    tryAdd(results, option);
                }
            }
            CompositeOptionFactory.build(
                    edgeBits,
                    edges.size(),
                    subset -> enumerate(subset, joinNodes),
                    candidate -> tryAdd(results, candidate));
        }

        List<KnownComponent> finalList = List.copyOf(results.values());
        memo.put(key, finalList);
        return finalList;
    }

    private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
        KnownComponent adjusted = candidateValidator.ensureLoopAnchored(candidate);
        if (!candidateValidator.matchesComponent(adjusted)) {
            return;
        }
        ComponentKey key = adjusted.toKey(edges.size());
        results.putIfAbsent(key, adjusted);
    }

    private Set<String> collectLocalJoinNodes(BitSet edgeBits, Set<String> joinNodes) {
        if (joinNodes == null || joinNodes.isEmpty()) {
            return Set.of();
        }
        Set<String> present = new HashSet<>();
        for (int idx = edgeBits.nextSetBit(0); idx >= 0; idx = edgeBits.nextSetBit(idx + 1)) {
            Edge edge = edges.get(idx);
            if (joinNodes.contains(edge.source())) {
                present.add(edge.source());
            }
            if (joinNodes.contains(edge.target())) {
                present.add(edge.target());
            }
            if (present.size() > 1) {
                // No need to continue once we exceed the backtrack-eligible threshold.
                break;
            }
        }
        return present.isEmpty() ? Set.of() : Set.copyOf(present);
    }

    private static final class MemoKey {
        private final String signature;
        private final JoinMode joinMode;

        MemoKey(String signature, JoinMode joinMode) {
            this.signature = signature;
            this.joinMode = joinMode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof MemoKey other)) {
                return false;
            }
            return joinMode == other.joinMode && signature.equals(other.signature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(signature, joinMode);
        }
    }

    private enum JoinMode {
        SINGLE(true),
        OTHER(false);

        private final boolean allowBacktracks;

        JoinMode(boolean allowBacktracks) {
            this.allowBacktracks = allowBacktracks;
        }

        static JoinMode fromCount(int joinNodeCount) {
            return joinNodeCount == 1 ? SINGLE : OTHER;
        }

        boolean allowBacktracks() {
            return allowBacktracks;
        }
    }

}
