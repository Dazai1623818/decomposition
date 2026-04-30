package evaluator.index;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Minimal boundary used by the evaluator engine to query CPQ-native indices.
 */
public interface CpqIndex {
    int k();

    int intersections();

    boolean isIndexable(CPQ cpq);

    /**
     * Returns whether this index can evaluate the provided CPQ considering
     * backend coverage, diameter, and intersection-width constraints.
     */
    default boolean supports(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        if (!isIndexable(cpq)) {
            return false;
        }
        int cap = intersections();
        if (cap == Integer.MAX_VALUE) {
            return true;
        }
        int maxWidth = maxIntersectionWidth(cpq.toAbstractSyntaxTree());
        return maxWidth <= cap;
    }

    long cost(CPQ cpq);

    List<Edge> query(CPQ cpq);

    /**
     * Returns primitive source/target matches for one indexed CPQ relation.
     * Implementations may override this to avoid building intermediate edge
     * objects when the backend already exposes native pairs.
     */
    default QueryMatches queryMatches(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return new EdgeQueryMatches(query(cpq));
    }

    /**
     * Planner-side component statistics for one indexed CPQ result relation.
     * Endpoint arrays are sorted, distinct, and must be treated as read-only.
     */
    default ComponentStats componentStats(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return ComponentStats.fromEdges(query(cpq));
    }

    /**
     * Compact arithmetic synopsis for one indexed CPQ result relation.
     */
    default RelationSynopsis componentSynopsis(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return componentStats(cpq).toSynopsis();
    }

    /**
     * Directed edge match between source and target graph nodes.
     */
    record Edge(int source, int target) {
    }

    @FunctionalInterface
    interface IntPairConsumer {
        void accept(int source, int target);
    }

    /**
     * Primitive match view used by execution-time relation compilation.
     */
    interface QueryMatches {
        int size();

        void forEach(IntPairConsumer consumer);
    }

    enum Endpoint {
        SOURCE,
        TARGET
    }

    /**
     * Compact endpoint synopsis used by arithmetic-only estimators.
     */
    record EndpointSummary(long distinctValues, double averageMultiplicity) {
        public EndpointSummary {
            if (distinctValues < 0L) {
                throw new IllegalArgumentException("distinctValues must be >= 0");
            }
            if (!Double.isFinite(averageMultiplicity) || averageMultiplicity < 0.0D) {
                throw new IllegalArgumentException("averageMultiplicity must be >= 0");
            }
        }

        public boolean isEmpty() {
            return distinctValues == 0L || averageMultiplicity <= 0.0D;
        }

        public static EndpointSummary empty() {
            return new EndpointSummary(0L, 0.0D);
        }
    }

    /**
     * Compact relation synopsis used by summary-based planning.
     */
    record RelationSynopsis(long tupleCount, EndpointSummary source, EndpointSummary target) {
        public RelationSynopsis {
            if (tupleCount < 0L) {
                throw new IllegalArgumentException("tupleCount must be >= 0");
            }
            source = source == null ? EndpointSummary.empty() : source;
            target = target == null ? EndpointSummary.empty() : target;
        }

        public boolean isEmpty() {
            return tupleCount == 0L || source.isEmpty() || target.isEmpty();
        }

        public EndpointSummary endpoint(Endpoint endpoint) {
            return endpoint == Endpoint.SOURCE ? source : target;
        }

        public long distinctValues(Endpoint endpoint) {
            return endpoint(endpoint).distinctValues();
        }

        public double averageMultiplicity(Endpoint endpoint) {
            return endpoint(endpoint).averageMultiplicity();
        }

        public static RelationSynopsis empty() {
            return new RelationSynopsis(0L, EndpointSummary.empty(), EndpointSummary.empty());
        }
    }

    record ComponentStats(long tupleCount, int[] sourceValues, int[] targetValues) {
        private static final int[] EMPTY_VALUES = new int[0];

        public ComponentStats {
            if (tupleCount < 0L) {
                throw new IllegalArgumentException("tupleCount must be >= 0");
            }
            sourceValues = normalizeValues(sourceValues);
            targetValues = normalizeValues(targetValues);
        }

        public boolean isEmpty() {
            return tupleCount == 0L || sourceValues.length == 0 || targetValues.length == 0;
        }

        public long distinctValues(Endpoint endpoint) {
            return endpoint == Endpoint.SOURCE ? sourceValues.length : targetValues.length;
        }

        public int[] values(Endpoint endpoint) {
            return endpoint == Endpoint.SOURCE ? sourceValues : targetValues;
        }

        /**
         * Builds the compact arithmetic synopsis used during planning.
         */
        public RelationSynopsis toSynopsis() {
            if (isEmpty()) {
                return RelationSynopsis.empty();
            }
            return new RelationSynopsis(
                    tupleCount,
                    endpointSummary(Endpoint.SOURCE),
                    endpointSummary(Endpoint.TARGET));
        }

        /**
         * Returns the compact endpoint synopsis for one component endpoint.
         */
        public EndpointSummary endpointSummary(Endpoint endpoint) {
            long distinct = distinctValues(endpoint);
            if (tupleCount == 0L || distinct == 0L) {
                return EndpointSummary.empty();
            }
            return new EndpointSummary(distinct, (double) tupleCount / (double) distinct);
        }

        public static ComponentStats empty() {
            return new ComponentStats(0L, EMPTY_VALUES, EMPTY_VALUES);
        }

        public static ComponentStats fromEdges(List<Edge> edges) {
            Objects.requireNonNull(edges, "edges");
            if (edges.isEmpty()) {
                return empty();
            }

            int[] sources = new int[edges.size()];
            int[] targets = new int[edges.size()];
            for (int i = 0; i < edges.size(); i++) {
                Edge edge = edges.get(i);
                sources[i] = edge.source();
                targets[i] = edge.target();
            }
            return new ComponentStats(edges.size(), sources, targets);
        }

        private static int[] normalizeValues(int[] values) {
            if (values == null || values.length == 0) {
                return EMPTY_VALUES;
            }
            int[] copy = Arrays.copyOf(values, values.length);
            Arrays.sort(copy);
            int unique = 1;
            for (int i = 1; i < copy.length; i++) {
                if (copy[i] != copy[unique - 1]) {
                    copy[unique++] = copy[i];
                }
            }
            return Arrays.copyOf(copy, unique);
        }
    }

    /**
     * Default primitive match view backed by boxed edge records.
     */
    final class EdgeQueryMatches implements QueryMatches {
        private final List<Edge> edges;

        private EdgeQueryMatches(List<Edge> edges) {
            this.edges = Objects.requireNonNull(edges, "edges");
        }

        @Override
        public int size() {
            return edges.size();
        }

        @Override
        public void forEach(IntPairConsumer consumer) {
            Objects.requireNonNull(consumer, "consumer");
            for (Edge edge : edges) {
                consumer.accept(edge.source(), edge.target());
            }
        }
    }

    /**
     * Computes the largest number of non-intersection operands inside any
     * intersection subtree.
     */
    private static int maxIntersectionWidth(QueryTree node) {
        int max = 0;
        if (node.getOperation() == OperationType.INTERSECTION) {
            max = intersectionWidth(node);
        }
        int arity = node.getArity();
        for (int i = 0; i < arity; i++) {
            max = Math.max(max, maxIntersectionWidth(node.getOperand(i)));
        }
        return max;
    }

    private static int intersectionWidth(QueryTree node) {
        int width = 0;
        ArrayDeque<QueryTree> stack = new ArrayDeque<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            QueryTree current = stack.pop();
            if (current.getOperation() == OperationType.INTERSECTION) {
                int arity = current.getArity();
                for (int i = 0; i < arity; i++) {
                    stack.push(current.getOperand(i));
                }
            } else {
                width++;
            }
        }
        return width;
    }
}
