package evaluator.index;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayDeque;
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
     * Directed edge match between source and target graph nodes.
     */
    record Edge(int source, int target) {
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
