package evaluator.decomposition;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Shared CPQ deduplication normalization used by decomposition methods.
 */
final class CpqDeduplication {
    private CpqDeduplication() {
    }

    static NormalizedCpq normalizeCpq(CPQ cpq) {
        CPQ normalized = normalizeTree(Objects.requireNonNull(cpq, "cpq").toAbstractSyntaxTree());
        return new NormalizedCpq(normalized, normalized.toString(), cpqSize(normalized));
    }

    private static int cpqSize(CPQ cpq) {
        return countNodes(cpq.toAbstractSyntaxTree());
    }

    private static int countNodes(QueryTree node) {
        int total = 1;
        for (int i = 0; i < node.getArity(); i++) {
            total += countNodes(node.getOperand(i));
        }
        return total;
    }

    private static CPQ normalizeTree(QueryTree node) {
        return switch (node.getOperation()) {
            case IDENTITY -> CPQ.id();
            case EDGE -> {
                Predicate label = node.getEdgeAtom().getLabel();
                yield CPQ.label(label);
            }
            case CONCATENATION -> {
                List<CPQ> parts = collectParts(node, OperationType.CONCATENATION);
                parts.removeIf(part -> part.getOperationType() == OperationType.IDENTITY);
                if (parts.isEmpty()) {
                    yield CPQ.id();
                }
                if (parts.size() == 1) {
                    yield parts.get(0);
                }

                CPQ normalized = parts.get(0);
                for (int i = 1; i < parts.size(); i++) {
                    normalized = CPQ.concat(normalized, parts.get(i));
                }
                yield normalized;
            }
            case INTERSECTION -> {
                List<CPQ> parts = collectParts(node, OperationType.INTERSECTION);
                Map<String, CPQ> unique = new HashMap<>();
                for (CPQ part : parts) {
                    unique.putIfAbsent(part.toString(), part);
                }

                List<Map.Entry<String, CPQ>> ordered = new ArrayList<>(unique.entrySet());
                ordered.sort(Comparator.comparing(Map.Entry::getKey));
                if (ordered.size() == 1) {
                    yield ordered.get(0).getValue();
                }

                CPQ normalized = ordered.get(0).getValue();
                for (int i = 1; i < ordered.size(); i++) {
                    normalized = CPQ.intersect(normalized, ordered.get(i).getValue());
                }
                yield normalized;
            }
            default -> throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
        };
    }

    /**
     * Flattens associative operations into left-to-right, normalized operands.
     */
    private static List<CPQ> collectParts(QueryTree node, OperationType op) {
        List<CPQ> parts = new ArrayList<>();
        ArrayDeque<QueryTree> stack = new ArrayDeque<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            QueryTree current = stack.pop();
            if (current.getOperation() == op) {
                stack.push(current.getOperand(1));
                stack.push(current.getOperand(0));
            } else {
                parts.add(normalizeTree(current));
            }
        }
        return parts;
    }

    record NormalizedCpq(CPQ cpq, String normalized, int size) {
    }
}
