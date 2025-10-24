package decomposition.cpq;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.ConcatCPQ;
import dev.roanh.gmark.lang.cpq.EdgeCPQ;
import dev.roanh.gmark.lang.cpq.IdentityCPQ;
import dev.roanh.gmark.lang.cpq.IntersectionCPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.generic.GenericConcatenation;
import dev.roanh.gmark.lang.generic.GenericEdge;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Validates whether a CPQ candidate actually matches the underlying component.
 */
final class ComponentCandidateValidator {

    private final List<Edge> edges;
    private static final Field EDGE_SYMBOL_FIELD;
    private static final Field CONCAT_ELEMENTS_FIELD;
    private static final Field INTERSECTION_ELEMENTS_FIELD;

    static {
        try {
            EDGE_SYMBOL_FIELD = GenericEdge.class.getDeclaredField("symbol");
            EDGE_SYMBOL_FIELD.setAccessible(true);
            CONCAT_ELEMENTS_FIELD = GenericConcatenation.class.getDeclaredField("elements");
            CONCAT_ELEMENTS_FIELD.setAccessible(true);
            INTERSECTION_ELEMENTS_FIELD = IntersectionCPQ.class.getDeclaredField("cpq");
            INTERSECTION_ELEMENTS_FIELD.setAccessible(true);
        } catch (NoSuchFieldException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    ComponentCandidateValidator(List<Edge> edges) {
        this.edges = edges;
    }

    List<KnownComponent> validateAndExpand(KnownComponent candidate) {
        KnownComponent anchored = ensureLoopAnchored(candidate);
        List<KnownComponent> variants = new ArrayList<>();
        if (matchesComponent(anchored)) {
            variants.add(anchored);
        }
        if (!anchored.source().equals(anchored.target())) {
            Optional<KnownComponent> reversed = reverseCandidate(anchored);
            if (reversed.isPresent()) {
                KnownComponent reversedAnchored = ensureLoopAnchored(reversed.get());
                if (matchesComponent(reversedAnchored)) {
                    variants.add(reversedAnchored);
                }
            }
        }
        return Collections.unmodifiableList(variants);
    }

    KnownComponent ensureLoopAnchored(KnownComponent candidate) {
        if (!candidate.source().equals(candidate.target())) {
            return candidate;
        }
        try {
            QueryGraphCPQ graph = candidate.cpq().toQueryGraph();
            if (graph.isLoop()) {
                return candidate;
            }
        } catch (RuntimeException ex) {
            return candidate;
        }
        try {
            CPQ anchored = CPQ.parse("(" + candidate.cpq().toString() + " ∩ id)");
            String derivation = candidate.derivation() + " + anchored with id";
            return new KnownComponent(
                    anchored,
                    candidate.edges(),
                    candidate.source(),
                    candidate.target(),
                    derivation);
        } catch (RuntimeException ex) {
            return candidate;
        }
    }

    private Optional<KnownComponent> reverseCandidate(KnownComponent candidate) {
        try {
            CPQ reversedCpq = reverse(candidate.cpq());
            if (reversedCpq == null) {
                return Optional.empty();
            }
            String derivation = candidate.derivation() + " + reversed orientation";
            return Optional.of(
                    new KnownComponent(
                            reversedCpq,
                            candidate.edges(),
                            candidate.target(),
                            candidate.source(),
                            derivation));
        } catch (UnsupportedOperationException | IllegalStateException ex) {
            return Optional.empty();
        }
    }

    private CPQ reverse(CPQ cpq) {
        if (cpq == null) {
            return null;
        }
        if (cpq instanceof IdentityCPQ) {
            return cpq;
        }
        if (cpq instanceof EdgeCPQ edge) {
            return reverseEdge(edge);
        }
        if (cpq instanceof ConcatCPQ concat) {
            List<CPQ> elements = concatElements(concat);
            List<CPQ> reversedElements = new ArrayList<>(elements.size());
            for (int i = elements.size() - 1; i >= 0; i--) {
                reversedElements.add(reverse(elements.get(i)));
            }
            return CPQ.concat(reversedElements);
        }
        if (cpq instanceof IntersectionCPQ intersection) {
            List<CPQ> operands = intersectionElements(intersection);
            List<CPQ> reversedOperands = new ArrayList<>(operands.size());
            for (CPQ operand : operands) {
                reversedOperands.add(reverse(operand));
            }
            return CPQ.intersect(reversedOperands);
        }
        throw new UnsupportedOperationException("Unsupported CPQ type: " + cpq.getClass().getName());
    }

    private CPQ reverseEdge(EdgeCPQ edge) {
        Predicate label = edgePredicate(edge);
        return CPQ.label(label.getInverse());
    }

    private Predicate edgePredicate(EdgeCPQ edge) {
        try {
            return (Predicate) EDGE_SYMBOL_FIELD.get(edge);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Failed to access edge predicate", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private List<CPQ> concatElements(ConcatCPQ concat) {
        try {
            List<CPQ> raw = (List<CPQ>) CONCAT_ELEMENTS_FIELD.get(concat);
            return new ArrayList<>(raw);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Failed to access concatenation elements", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private List<CPQ> intersectionElements(IntersectionCPQ intersection) {
        try {
            List<CPQ> raw = (List<CPQ>) INTERSECTION_ELEMENTS_FIELD.get(intersection);
            return new ArrayList<>(raw);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Failed to access intersection elements", ex);
        }
    }

    boolean matchesComponent(KnownComponent candidate) {
        List<Edge> componentEdges = edgesFor(candidate.edges());
        if (componentEdges.isEmpty() || !coversEndpoints(candidate, componentEdges)) {
            return false;
        }

        ParsedCandidate parsed = parseCandidate(candidate);
        if (parsed == null || parsed.cpqEdges().size() != componentEdges.size()) {
            return false;
        }

        boolean candidateIsLoop = candidate.source().equals(candidate.target());
        if (parsed.cpqGraph().isLoop() != candidateIsLoop) {
            return false;
        }

        MatchContext context = MatchContext.create(parsed, componentEdges, candidate);
        return matchEdges(0, context);
    }

    private boolean coversEndpoints(KnownComponent candidate, List<Edge> componentEdges) {
        LinkedHashSet<String> vertices = new LinkedHashSet<>();
        for (Edge edge : componentEdges) {
            vertices.add(edge.source());
            vertices.add(edge.target());
        }
        return vertices.contains(candidate.source()) && vertices.contains(candidate.target());
    }

    private ParsedCandidate parseCandidate(KnownComponent candidate) {
        try {
            CPQ cpq = candidate.cpq();
            CQ cqPattern = cpq.toCQ();
            QueryGraphCQ cqGraph = cqPattern.toQueryGraph();
            UniqueGraph<VarCQ, AtomCQ> graph = cqGraph.toUniqueGraph();
            QueryGraphCPQ cpqGraph = cpq.toQueryGraph();
            String sourceVarName = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
            String targetVarName = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());
            return new ParsedCandidate(graph.getEdges(), cpqGraph, sourceVarName, targetVarName);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private List<Edge> edgesFor(BitSet bits) {
        List<Edge> selected = new ArrayList<>(bits.cardinality());
        BitsetUtils.stream(bits).forEach(idx -> selected.add(edges.get(idx)));
        return selected;
    }

    private boolean matchEdges(int index, MatchContext context) {
        if (index == context.cpqEdges().size()) {
            String mappedSource = context.variableMapping().get(context.sourceVarName());
            String mappedTarget = context.variableMapping().get(context.targetVarName());
            return context.remaining().isEmpty()
                    && context.expectedSource().equals(mappedSource)
                    && context.expectedTarget().equals(mappedTarget);
        }

        GraphEdge<VarCQ, AtomCQ> cpqEdge = context.cpqEdges().get(index);
        AtomCQ atom = cpqEdge.getData();
        String label = atom.getLabel().getAlias();
        String cpqSrcName = cpqEdge.getSourceNode().getData().getName();
        String cpqTrgName = cpqEdge.getTargetNode().getData().getName();

        for (int i = 0; i < context.remaining().size(); i++) {
            Edge edge = context.remaining().get(i);
            if (!label.equals(edge.label())) {
                continue;
            }
            String componentSource = edge.source();
            String componentTarget = edge.target();

            String mappedSrc = context.variableMapping().get(cpqSrcName);
            String mappedTrg = context.variableMapping().get(cpqTrgName);

            if (mappedSrc != null && !mappedSrc.equals(componentSource)) {
                continue;
            }
            if (mappedTrg != null && !mappedTrg.equals(componentTarget)) {
                continue;
            }
            if (mappedSrc == null && context.usedNodes().contains(componentSource)) {
                continue;
            }
            if (mappedTrg == null && context.usedNodes().contains(componentTarget)) {
                continue;
            }

            boolean addedSrc = false;
            boolean addedTrg = false;
            if (mappedSrc == null) {
                context.variableMapping().put(cpqSrcName, componentSource);
                context.usedNodes().add(componentSource);
                addedSrc = true;
            }
            if (mappedTrg == null) {
                context.variableMapping().put(cpqTrgName, componentTarget);
                context.usedNodes().add(componentTarget);
                addedTrg = true;
            }

            Edge removed = context.remaining().remove(i);
            if (matchEdges(index + 1, context)) {
                return true;
            }

            context.remaining().add(i, removed);
            if (addedSrc) {
                context.variableMapping().remove(cpqSrcName);
                context.usedNodes().remove(componentSource);
            }
            if (addedTrg) {
                context.variableMapping().remove(cpqTrgName);
                context.usedNodes().remove(componentTarget);
            }
        }

        return false;
    }

    private record ParsedCandidate(List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
                                   QueryGraphCPQ cpqGraph,
                                   String sourceVarName,
                                   String targetVarName) {
    }

    /**
     * Mutable traversal state for validating a candidate against the component edges.
     * The data structure mirrors the recursive backtracking expectation and keeps parameter lists short.
     */
    private record MatchContext(List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
                                List<Edge> remaining,
                                Map<String, String> variableMapping,
                                Set<String> usedNodes,
                                String sourceVarName,
                                String targetVarName,
                                String expectedSource,
                                String expectedTarget) {

        static MatchContext create(ParsedCandidate parsed,
                                   List<Edge> componentEdges,
                                   KnownComponent candidate) {
            Map<String, String> variables = new HashMap<>();
            Set<String> used = new HashSet<>();
            variables.put(parsed.sourceVarName(), candidate.source());
            variables.put(parsed.targetVarName(), candidate.target());
            used.add(candidate.source());
            used.add(candidate.target());
            return new MatchContext(
                    parsed.cpqEdges(),
                    new ArrayList<>(componentEdges),
                    variables,
                    used,
                    parsed.sourceVarName(),
                    parsed.targetVarName(),
                    candidate.source(),
                    candidate.target());
        }
    }
}
