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

        Map<String, KnownComponent> results = new LinkedHashMap<>();
        int cardinality = edgeBits.cardinality();

        if (cardinality == 0) {
            memo.put(signature, List.of());
            return memo.get(signature);
        }

        if (cardinality == 1) {
            int edgeIndex = edgeBits.nextSetBit(0);
            Edge edge = edges.get(edgeIndex);
            BitSet bitsCopy = BitsetUtils.copy(edgeBits);
            KnownComponent forward = new KnownComponent(true, CPQStringBuilder.atom(edge.label()), bitsCopy, edge.source(), edge.target());
            tryAdd(results, forward);
        } else {
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
                        if (left.target().equals(right.source())) {
                            String rule = CPQStringBuilder.concat(left.cpqRule(), right.cpqRule());
                            KnownComponent combined = new KnownComponent(true, rule, BitsetUtils.copy(edgeBits), left.source(), right.target());
                            tryAdd(results, combined);
                        }
                        if (left.source().equals(right.source()) && left.target().equals(right.target())) {
                            String rule = CPQStringBuilder.conjunction(left.cpqRule(), right.cpqRule());
                            KnownComponent combined = new KnownComponent(true, rule, BitsetUtils.copy(edgeBits), left.source(), left.target());
                            tryAdd(results, combined);
                        }
                    }
                }
            }
        }

        List<KnownComponent> snapshot = new ArrayList<>(results.values());
        List<KnownComponent> additional = new ArrayList<>();
        for (KnownComponent candidate : snapshot) {
            if (candidate.source().equals(candidate.target())) {
                String anchored = CPQStringBuilder.withId(candidate.cpqRule());
                KnownComponent anchoredComponent = new KnownComponent(true, anchored, candidate.edges(), candidate.source(), candidate.target());
                additional.add(anchoredComponent);
            }
            String inverseRule = CPQStringBuilder.inverse(candidate.cpqRule());
            KnownComponent inverseComponent = new KnownComponent(true, inverseRule, candidate.edges(), candidate.target(), candidate.source());
            additional.add(inverseComponent);
        }
        for (KnownComponent extra : additional) {
            tryAdd(results, extra);
        }

        List<KnownComponent> finalList = List.copyOf(results.values());
        memo.put(signature, finalList);
        return finalList;
    }

    private void tryAdd(Map<String, KnownComponent> results, KnownComponent candidate) {
        try {
            CPQ.parse(candidate.cpqRule());
        } catch (RuntimeException ex) {
            return;
        }
        String key = candidate.source() + "|" + candidate.target() + "|" + candidate.cpqRule();
        results.putIfAbsent(key, candidate);
    }
}
