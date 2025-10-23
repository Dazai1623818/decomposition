package decomposition.cpq;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Builds CPQ expressions for connected components using gMark's CPQ model.
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
        Set<String> localJoinNodes = collectLocalJoinNodes(edgeBits, joinNodes);
        MemoKey key = new MemoKey(BitsetUtils.signature(edgeBits, edges.size()), localJoinNodes);
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
            SingleEdgeOptionFactory.build(edge, edgeBits)
                    .forEach(option -> tryAdd(results, option));
        } else {
            if (localJoinNodes.size() <= 1) {
                LoopBacktrackBuilder.build(edges, edgeBits, localJoinNodes)
                        .forEach(option -> tryAdd(results, option));
            }

            Function<BitSet, List<KnownComponent>> lookup = subset -> enumerate(subset, joinNodes);
            CompositeOptionFactory.build(edgeBits, edges.size(), lookup, candidate -> tryAdd(results, candidate));
        }

        List<KnownComponent> finalList = List.copyOf(results.values());
        memo.put(key, finalList);
        return finalList;
    }

    private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
        for (KnownComponent variant : candidateValidator.validateAndExpand(candidate)) {
            ComponentKey key = variant.toKey(edges.size());
            results.putIfAbsent(key, variant);
        }
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
        }
        return present.isEmpty() ? Set.of() : Set.copyOf(present);
    }

    private static final class MemoKey {
        private final String signature;
        private final Set<String> anchors;

        MemoKey(String signature, Set<String> anchors) {
            this.signature = signature;
            this.anchors = anchors;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof MemoKey other)) {
                return false;
            }
            return signature.equals(other.signature) && anchors.equals(other.anchors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(signature, anchors);
        }
    }
}
