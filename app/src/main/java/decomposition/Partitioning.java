package decomposition;

import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.lang.cq.CQ;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Partitioning {

    public static class Edge {
        public final String source;
        public final String target;
        public final Predicate predicate;

        public Edge(String source, String target, Predicate predicate) {
            this.source = source;
            this.target = target;
            this.predicate = predicate;
        }

        public Predicate getPredicate() {
            return predicate;
        }

        public String label() {
            return predicate.getAlias();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Edge edge = (Edge) o;
            return source.equals(edge.source) &&
                target.equals(edge.target) &&
                predicate.equals(edge.predicate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, target, predicate);
        }

        @Override
        public String toString() {
            return String.format("%s --%s--> %s", source, predicate.getAlias(), target);
        }
    }

    public static List<List<List<Edge>>> enumerateConnectedEdgePartitions(CQ cq) {
        List<Edge> edges = QueryUtils.extractEdgesFromCQ(cq);
        List<List<List<Edge>>> result = new ArrayList<>();
        findPartitions(edges, new ArrayList<>(), new HashSet<>(), result);
        result.sort(partitionOrderComparator()); // sort here
        return result;
    }

    private static void findPartitions(List<Edge> remaining, List<List<Edge>> current, Set<String> memo, List<List<List<Edge>>> result) {
        if (remaining.isEmpty()) {
            String key = canonicalPartition(current);
            if (memo.add(key)) result.add(new ArrayList<>(current));
            return;
        }

        for (List<Edge> subset : generateConnectedSubsets(remaining)) {
            List<Edge> nextRemaining = listSubtract(remaining, subset);
            current.add(subset);
            findPartitions(nextRemaining, current, memo, result);
            current.remove(current.size() - 1);
        }
    }

    public static List<List<Edge>> generateConnectedSubsets(List<Edge> edges) {
        List<List<Edge>> subsets = new ArrayList<>();
        int n = edges.size();

        for (int i = 1; i < (1 << n); i++) {
            List<Edge> subset = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) != 0) {
                    subset.add(edges.get(j));
                }
            }
            if (isConnected(subset)) subsets.add(subset);
        }
        return subsets;
    }

    public static boolean isConnected(List<Edge> edges) {
        if (edges.isEmpty()) return false;

        Map<String, Set<String>> graph = new HashMap<>();
        for (Edge e : edges) {
            graph.computeIfAbsent(e.source, k -> new HashSet<>()).add(e.target);
            graph.computeIfAbsent(e.target, k -> new HashSet<>()).add(e.source);
        }

        Set<String> visited = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>();
        stack.push(edges.get(0).source);

        while (!stack.isEmpty()) {
            String node = stack.pop();
            if (!visited.add(node)) continue;
            for (String neighbor : graph.getOrDefault(node, Set.of())) {
                if (!visited.contains(neighbor)) stack.push(neighbor);
            }
        }

        Set<String> allNodes = edges.stream()
                .flatMap(e -> Stream.of(e.source, e.target))
                .collect(Collectors.toSet());

        return visited.equals(allNodes);
    }



    public static List<Edge> listSubtract(List<Edge> a, List<Edge> b) {
        List<Edge> result = new ArrayList<>(a);
        for (Edge e : b) result.remove(e); // remove once, preserving multiplicity
        return result;
    }

    public static String canonicalPartition(List<List<Edge>> partition) {
        return partition.stream()
                .map(comp -> comp.stream()
                        .sorted(Comparator.comparing(Object::toString))
                        .map(Object::toString)
                        .collect(Collectors.joining(",")))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    public static Set<String> getVertices(List<Edge> edges) {
        return edges.stream()
                .flatMap(e -> Stream.of(e.source, e.target))
                .collect(Collectors.toSet());
    }

    public static List<Set<String>> getJoinNodesPerComponent(List<List<Edge>> partition, Set<String> freeVariables) {
        List<Set<String>> componentVertices = partition.stream()
                .map(Partitioning::getVertices)
                .toList();

        List<Set<String>> result = new ArrayList<>();
        for (int i = 0; i < componentVertices.size(); i++) {
            Set<String> others = new HashSet<>(freeVariables);
            for (int j = 0; j < componentVertices.size(); j++) {
                if (j != i) others.addAll(componentVertices.get(j));
            }
            Set<String> intersection = new HashSet<>(componentVertices.get(i));
            intersection.retainAll(others);
            result.add(intersection);
        }
        return result;
    }

    public static List<List<List<Edge>>> filterPartitionsByJoinConstraint(
            List<List<List<Edge>>> partitions,
            int maxJoinNodesPerComponent,
            Set<String> freeVariables) {

        return partitions.stream()
                .filter(partition -> getJoinNodesPerComponent(partition, freeVariables).stream()
                        .allMatch(jn -> jn.size() <= maxJoinNodesPerComponent))
                .collect(Collectors.toList());
    }
    public static Comparator<List<List<Edge>>> partitionOrderComparator() {
    return Comparator
            .comparing((List<List<Edge>> p) ->
                    p.stream().mapToInt(List::size).max().orElse(0)) // max component size
            .thenComparing(p ->
                    Collections.frequency(
                            p.stream().map(List::size).toList(),
                            p.stream().mapToInt(List::size).max().orElse(0))) // freq of max
            .thenComparing(Partitioning::canonicalPartition); // tiebreaker
    }


}
