package decomposition.partitions;

import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.GraphUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Enumerates connected edge components and partitions that cover the entire CQ.
 */
public final class PartitionGenerator {
    private final int maxPartitions;

    public PartitionGenerator(int maxPartitions) {
        this.maxPartitions = maxPartitions;
    }

    public List<Component> enumerateConnectedComponents(List<Edge> edges) {
        Objects.requireNonNull(edges, "edges");
        int edgeCount = edges.size();
        List<Component> components = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
            BitSet seed = new BitSet(edgeCount);
            seed.set(edgeIndex);
            expand(seed, edges, components, seen);
        }

        return components;
    }

    public List<Partition> enumeratePartitions(List<Edge> edges, List<Component> components) {
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(components, "components");

        int edgeCount = edges.size();
        BitSet allEdges = BitsetUtils.allOnes(edgeCount);
        Map<Integer, List<Component>> componentsByEdge = indexComponentsByEdge(edgeCount, components);

        List<Partition> partitions = new ArrayList<>();
        backtrack(allEdges, new BitSet(edgeCount), new ArrayList<>(), partitions, componentsByEdge);
        return partitions;
    }

    private void expand(BitSet current,
                        List<Edge> edges,
                        List<Component> components,
                        Set<String> seen) {
        String signature = BitsetUtils.signature(current, edges.size());
        if (!seen.add(signature)) {
            return;
        }

        BitSet snapshot = (BitSet) current.clone();
        components.add(GraphUtils.buildComponent(snapshot, edges));

        Set<Integer> expandable = expandableEdges(current, edges);
        for (Integer candidate : expandable) {
            current.set(candidate);
            expand(current, edges, components, seen);
            current.clear(candidate);
        }
    }

    private Set<Integer> expandableEdges(BitSet current, List<Edge> edges) {
        Set<Integer> result = new HashSet<>();
        Set<String> frontier = GraphUtils.vertices(current, edges);

        for (int idx = 0; idx < edges.size(); idx++) {
            if (current.get(idx)) {
                continue;
            }
            Edge edge = edges.get(idx);
            if (frontier.contains(edge.source()) || frontier.contains(edge.target())) {
                result.add(idx);
            }
        }
        return result;
    }

    private Map<Integer, List<Component>> indexComponentsByEdge(int edgeCount, List<Component> components) {
        Map<Integer, List<Component>> map = new HashMap<>();
        for (int i = 0; i < edgeCount; i++) {
            map.put(i, new ArrayList<>());
        }
        for (Component component : components) {
            BitSet bits = component.edgeBits();
            for (int edgeIndex = bits.nextSetBit(0); edgeIndex >= 0; edgeIndex = bits.nextSetBit(edgeIndex + 1)) {
                map.get(edgeIndex).add(component);
            }
        }
        return map;
    }

    private void backtrack(BitSet allEdges,
                           BitSet used,
                           List<Component> chosen,
                           List<Partition> output,
                           Map<Integer, List<Component>> componentsByEdge) {
        if (used.equals(allEdges)) {
            output.add(new Partition(chosen));
            return;
        }

        if (maxPartitions > 0 && output.size() >= maxPartitions) {
            return;
        }

        int nextEdge = used.nextClearBit(0);
        List<Component> candidates = componentsByEdge.getOrDefault(nextEdge, List.of());
        for (Component candidate : candidates) {
            BitSet bits = candidate.edgeBits();
            BitSet overlap = (BitSet) bits.clone();
            overlap.and(used);
            if (!overlap.isEmpty()) {
                continue;
            }
            BitSet nextUsed = (BitSet) used.clone();
            nextUsed.or(bits);
            List<Component> nextChosen = new ArrayList<>(chosen);
            nextChosen.add(candidate);
            backtrack(allEdges, nextUsed, nextChosen, output, componentsByEdge);
        }
    }
}
