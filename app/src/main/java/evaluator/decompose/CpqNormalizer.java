package evaluator.decompose;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.QueryLanguageSyntax;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

final class CpqNormalizer {
    record Normalized(CPQ cpq, String canonical) {
    }

    record Timing(long calls, long totalNanos) {
    }

    private static final LongAdder TIMING_CALLS = new LongAdder();
    private static final LongAdder TIMING_NANOS = new LongAdder();
    private static volatile boolean timingEnabled = false;

    static void setTimingEnabled(boolean enabled) {
        timingEnabled = enabled;
    }

    static void resetTiming() {
        TIMING_CALLS.reset();
        TIMING_NANOS.reset();
    }

    static Timing timingSnapshot() {
        return new Timing(TIMING_CALLS.sum(), TIMING_NANOS.sum());
    }

    static void printStats() {
        long calls = TIMING_CALLS.sum();
        long nanos = TIMING_NANOS.sum();
        if (calls == 0) {
            System.out.println("Normalization: no calls");
        } else {
            System.out.println(String.format("Normalization: calls=%d, total=%.3fms, avg=%.3fus",
                    calls,
                    nanos / 1_000_000.0,
                    (nanos / (double) calls) / 1_000.0));
        }
    }

    static Normalized normalize(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        if (!timingEnabled) {
            return normalizeTree(cpq.toAbstractSyntaxTree());
        }

        long start = System.nanoTime();
        try {
            return normalizeTree(cpq.toAbstractSyntaxTree());
        } finally {
            TIMING_CALLS.increment();
            TIMING_NANOS.add(System.nanoTime() - start);
        }
    }

    private static Normalized normalizeTree(QueryTree node) {
        return switch (node.getOperation()) {
            case IDENTITY -> new Normalized(CPQ.id(), "id");
            case EDGE -> normalizeEdge(node);
            case CONCATENATION -> normalizeConcatenation(node);
            case INTERSECTION -> normalizeIntersection(node);
            default -> throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
        };
    }

    private static Normalized normalizeEdge(QueryTree node) {
        Predicate label = node.getEdgeAtom().getLabel();
        return new Normalized(CPQ.label(label), label.getAlias());
    }

    private static void collectConcatenation(QueryTree node, List<Normalized> out) {
        if (node.getOperation() == OperationType.CONCATENATION) {
            collectConcatenation(node.getOperand(0), out);
            collectConcatenation(node.getOperand(1), out);
            return;
        }
        out.add(normalizeTree(node));
    }

    private static Normalized normalizeConcatenation(QueryTree node) {
        List<Normalized> parts = new ArrayList<>();
        collectConcatenation(node, parts);

        parts.removeIf(part -> part.cpq().getOperationType() == OperationType.IDENTITY);
        if (parts.isEmpty()) {
            return new Normalized(CPQ.id(), "id");
        }
        if (parts.size() == 1) {
            return parts.get(0);
        }

        CPQ normalized = foldConcat(parts);
        String canonical = "(" + parts.stream()
                .map(Normalized::canonical)
                .collect(Collectors.joining(String.valueOf(QueryLanguageSyntax.CHAR_JOIN))) + ")";
        return new Normalized(normalized, canonical);
    }

    private static CPQ foldConcat(List<Normalized> parts) {
        CPQ cpq = parts.get(0).cpq();
        for (int i = 1; i < parts.size(); i++) {
            cpq = CPQ.concat(cpq, parts.get(i).cpq());
        }
        return cpq;
    }

    private static void collectIntersection(QueryTree node, List<Normalized> out) {
        if (node.getOperation() == OperationType.INTERSECTION) {
            collectIntersection(node.getOperand(0), out);
            collectIntersection(node.getOperand(1), out);
            return;
        }
        out.add(normalizeTree(node));
    }

    private static Normalized normalizeIntersection(QueryTree node) {
        List<Normalized> parts = new ArrayList<>();
        collectIntersection(node, parts);

        Map<String, Normalized> unique = new HashMap<>();
        for (Normalized part : parts) {
            unique.putIfAbsent(part.canonical(), part);
        }

        List<Normalized> ordered = new ArrayList<>(unique.values());
        ordered.sort(Comparator.comparing(Normalized::canonical));
        if (ordered.size() == 1) {
            return ordered.get(0);
        }

        CPQ normalized = foldIntersect(ordered);
        String canonical = "(" + ordered.stream()
                .map(Normalized::canonical)
                .collect(Collectors.joining(String.valueOf(QueryLanguageSyntax.CHAR_INTERSECTION))) + ")";
        return new Normalized(normalized, canonical);
    }

    private static CPQ foldIntersect(List<Normalized> parts) {
        CPQ cpq = parts.get(0).cpq();
        for (int i = 1; i < parts.size(); i++) {
            cpq = CPQ.intersect(cpq, parts.get(i).cpq());
        }
        return cpq;
    }
}
