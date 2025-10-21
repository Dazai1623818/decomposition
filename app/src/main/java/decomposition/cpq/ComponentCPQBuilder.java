package decomposition.cpq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;

/**
 * Builds CPQ expressions for connected components using gMark's CPQ model.
 *
 * Design principles:
 * - Every CPQ records the concrete traversal direction via its source → target
 * - Reverse directionality is expressed via inverse LABELS (r⁻) when needed
 * - Components may expose both forward and inverse orientations when the CQ permits it
 * - Matching is strictly directional
 */
public final class ComponentCPQBuilder {
    private final List<Edge> edges;
    private final Map<String, List<KnownComponent>> memo = new HashMap<>();

    public ComponentCPQBuilder(List<Edge> edges) {
        this.edges = List.copyOf(edges);
    }

    int totalEdges() {
        return edges.size();
    }

    /**
     * Deprecated: do not rely on early ‘first candidate’ semantics.
     */
    @Deprecated(forRemoval = false)
    Optional<KnownComponent> build(BitSet edgeBits) {
        List<KnownComponent> candidates = options(edgeBits);
        return candidates.isEmpty() ? Optional.empty() : Optional.of(candidates.get(0));
    }

    public List<KnownComponent> options(BitSet edgeBits) {
        return enumerate(edgeBits);
    }

    List<KnownComponent> enumerateAll(BitSet edgeBits) {
        return options(edgeBits);
    }

    private List<KnownComponent> enumerate(BitSet edgeBits) {
        String signature = BitsetUtils.signature(edgeBits, edges.size());
        if (memo.containsKey(signature)) {
            return memo.get(signature);
        }

        Map<String, KnownComponent> results = new LinkedHashMap<>();
        int cardinality = edgeBits.cardinality();

        if (cardinality == 0) {
            List<KnownComponent> emptyList = List.of();
            memo.put(signature, emptyList);
            return emptyList;
        }

        if (cardinality == 1) {
            int edgeIndex = edgeBits.nextSetBit(0);
            Edge edge = edges.get(edgeIndex);
            BitSet bitsCopy = BitsetUtils.copy(edgeBits);

            // 0) Plain forward atom: s --r--> t
            CPQ forwardCPQ = CPQ.parse(edge.label());
            KnownComponent forward = derived(
                forwardCPQ,
                bitsCopy,
                edge.source(),
                edge.target(),
                "Forward atom on label '" + edge.label() + "' (" + edge.source() + "→" + edge.target() + ")"
            );
            tryAdd(results, forward);

            // 0a) Inverse atom: t --r⁻--> s (only when endpoints differ)
            if (!edge.source().equals(edge.target())) {
                String inverseStr = edge.label() + "⁻";
                try {
                    CPQ inverseCPQ = CPQ.parse(inverseStr);
                    KnownComponent inverse = derived(
                        inverseCPQ,
                        bitsCopy,
                        edge.target(),
                        edge.source(),
                        "Inverse atom on label '" + edge.label() + "' (" + edge.target() + "→" + edge.source() + ")"
                    );
                    tryAdd(results, inverse);
                } catch (RuntimeException ex) {
                    // Skip if inverse parsing fails
                }
            }

            // 1) Enrich single-edge case with anchored backtrack/self-loop forms only.
            addSingleEdgeBacktrackVariants(results, edge, bitsCopy);

        } else {
            // unchanged: your existing composition logic
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
                        // concatenation: shared middle
                        if (left.target().equals(right.source())) {
                            String concatStr = "(" + left.cpq().toString() + " ◦ " + right.cpq().toString() + ")";
                            CPQ concatCPQ = CPQ.parse(concatStr);
                            String derivation = "Concatenation: [" + left.cpqRule() + "] then [" + right.cpqRule()
                                    + "] via " + left.target();
                            KnownComponent combined = derived(
                                concatCPQ, BitsetUtils.copy(edgeBits), left.source(), right.target(), derivation
                            );
                            tryAdd(results, combined);
                        }
                        // conjunction: same endpoints
                        if (left.source().equals(right.source()) && left.target().equals(right.target())) {
                            String conjStr = "(" + left.cpq().toString() + " ∩ " + right.cpq().toString() + ")";
                            CPQ conjCPQ = CPQ.parse(conjStr);
                            String derivation = "Intersection: [" + left.cpqRule() + "] ∩ [" + right.cpqRule()
                                    + "] at " + left.source() + "→" + left.target();
                            KnownComponent combined = derived(
                                conjCPQ, BitsetUtils.copy(edgeBits), left.source(), left.target(), derivation
                            );
                            tryAdd(results, combined);
                        }
                    }
                }
            }

            // Multi-edge backtrack variants: extend backtrack patterns when cardinality > 1
            addMultiEdgeBacktrackVariants(results, edgeBits);
        }

        // Post-processing: add anchored variants for same-node components if not already anchored.
        // REMOVED: Inverse CPQ generation - reverse direction is only supported via inverse labels (r⁻),
        // not by reversing entire CPQs and swapping endpoints.
        List<KnownComponent> snapshot = new ArrayList<>(results.values());
        List<KnownComponent> additional = new ArrayList<>();
        for (KnownComponent candidate : snapshot) {
            // Only add ... ∩ id if it's a same-node component AND not already anchored
            if (candidate.source().equals(candidate.target()) && !looksAnchored(candidate.cpq())) {
                String anchoredStr = candidate.cpq().toString() + " ∩ id";
                try {
                    CPQ anchoredCPQ = CPQ.parse(anchoredStr);
                    String derivation = "Anchor with id: [" + candidate.cpqRule() + "] ∩ id at "
                            + candidate.source();
                    KnownComponent anchoredComponent = derived(
                        anchoredCPQ, candidate.edges(), candidate.source(), candidate.target(), derivation
                    );
                    additional.add(anchoredComponent);
                } catch (RuntimeException ex) {
                    // Skip if anchoring fails
                }
            }
        }
        for (KnownComponent extra : additional) {
            tryAdd(results, extra);
        }

        List<KnownComponent> ordered = new ArrayList<>(results.values());
        ordered.sort(CANDIDATE_COMPARATOR);
        List<KnownComponent> deduped = dedupe(ordered);
        List<KnownComponent> finalList = List.copyOf(deduped);
        memo.put(signature, finalList);
        return finalList;
    }

    /**
     * Adds backtrack variants using inverse labels (r⁻) within CPQs for SINGLE edges only.
     * Variants remain anchored at the original source or target through identity intersections.
     *
     * Generates only:
     *  - single backtracks: (r◦r⁻)∩id anchored at source, (r⁻◦r)∩id anchored at target
     *  - selfloop: if source==target also add (r)∩id
     *
     * Multi-edge backtracks (double, multi) are handled separately during composition
     * when cardinality > 1, as they represent extending the backtrack case.
     *
     * Note: r⁻ is an inverse LABEL, not a reversed CPQ. Endpoints are never swapped.
     */
    private void addSingleEdgeBacktrackVariants(Map<String, KnownComponent> results, Edge e, BitSet bits) {
        String s = e.source();
        String t = e.target();
        String r = e.label();

        // --- single backtracks (anchored) ---
        // (r◦r⁻) ∩ id at source
        String singleSStr = "(" + r + " ◦ (" + r + ")⁻) ∩ id";
        try {
            CPQ singleSCPQ = CPQ.parse(singleSStr);
            String derivation = "Single backtrack anchored at " + s + " via " + singleSStr;
            tryAdd(results, derived(singleSCPQ, bits, s, s, derivation));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // (r⁻◦r) ∩ id at target
        String singleTStr = "((" + r + ")⁻ ◦ " + r + ") ∩ id";
        try {
            CPQ singleTCPQ = CPQ.parse(singleTStr);
            String derivation = "Single backtrack anchored at " + t + " via " + singleTStr;
            tryAdd(results, derived(singleTCPQ, bits, t, t, derivation));
        } catch (RuntimeException ex) {
            // Skip if parsing fails
        }

        // --- self-loop convenience: if s==t, also expose r∩id explicitly ---
        if (s.equals(t)) {
            String rAnchoredStr = r + " ∩ id";
            try {
                CPQ rAnchoredCPQ = CPQ.parse(rAnchoredStr);
                String derivation = "Self-loop anchor at " + s + " exposing " + rAnchoredStr;
                tryAdd(results, derived(rAnchoredCPQ, bits, s, s, derivation));
            } catch (RuntimeException ex) {
                // Skip if parsing fails
            }
        }
    }

    /**
     * Adds multi-edge backtrack variants when cardinality > 1.
     * These patterns extend the single backtrack case by wrapping existing components.
     *
     * Generates patterns like:
     *  - (r◦q◦r⁻)∩id where q is an anchored component and r is from decomposition
     *  - Double backtracks and higher-order patterns via composition
     *
     * This is separate from concat and conjunction as it represents extending
     * the backtrack case with multiple edge traversals.
     */
    private void addMultiEdgeBacktrackVariants(Map<String, KnownComponent> results, BitSet edgeBits) {
        List<KnownComponent> snapshot = new ArrayList<>(results.values());

        for (KnownComponent component : snapshot) {
            // For each component that uses these edges, try to find single-edge subsets
            // that could wrap around it for backtracking patterns
            List<Integer> indices = BitsetUtils.toIndexList(edgeBits);

            for (int edgeIndex : indices) {
                Edge edge = edges.get(edgeIndex);
                String r = edge.label();
                String s = edge.source();
                String t = edge.target();

                // Pattern: r ◦ q ◦ r⁻ anchored at source
                // where q is anchored at target and uses remaining edges
                if (component.source().equals(t) && component.target().equals(t)) {
                    String backtrackStr = "(" + r + " ◦ " + component.cpq().toString() + " ◦ (" + r + ")⁻) ∩ id";
                    try {
                        CPQ backtrackCPQ = CPQ.parse(backtrackStr);
                        String derivation = "Extended backtrack at " + s + " wrapping [" + component.cpqRule() + "]";
                        tryAdd(results, derived(backtrackCPQ, edgeBits, s, s, derivation));
                    } catch (RuntimeException ex) {
                        // Skip if parsing fails
                    }
                }

                // Symmetric pattern: r⁻ ◦ q ◦ r anchored at target
                // where q is anchored at source and uses remaining edges
                if (component.source().equals(s) && component.target().equals(s)) {
                    String backtrackStr = "((" + r + ")⁻ ◦ " + component.cpq().toString() + " ◦ " + r + ") ∩ id";
                    try {
                        CPQ backtrackCPQ = CPQ.parse(backtrackStr);
                        String derivation = "Extended backtrack at " + t + " wrapping [" + component.cpqRule() + "]";
                        tryAdd(results, derived(backtrackCPQ, edgeBits, t, t, derivation));
                    } catch (RuntimeException ex) {
                        // Skip if parsing fails
                    }
                }
            }
        }
    }

    /**
     * Lightweight detector to avoid ((...) ∩ id) ∩ id duplicates.
     * Checks if the CPQ is already anchored with identity based on its string representation.
     */
    private boolean looksAnchored(CPQ cpq) {
        String str = cpq.toString().replace(" ", "");
        return str.endsWith("∩id") || str.endsWith("∩id)") || str.contains(")∩id");
    }

    private KnownComponent derived(CPQ cpq, BitSet bits, String source, String target, String derivation) {
        return new KnownComponent(cpq, bits, source, target, derivation);
    }

    private void tryAdd(Map<String, KnownComponent> results, KnownComponent candidate) {
        // Validate that the CPQ is valid (it should be since we built it via CPQ.parse)
        try {
            candidate.cpq().toString(); // Just ensure it's valid
            if (!matchesComponent(candidate)) {
                return;
            }
        } catch (RuntimeException ex) {
            // Skip invalid CPQs
            return;
        }
        String key = canonicalKey(candidate);
        KnownComponent existing = results.get(key);
        if (existing == null) {
            results.put(key, candidate);
            return;
        }
        if (shouldReplace(existing, candidate)) {
            results.put(key, candidate);
        }
    }

    private boolean shouldReplace(KnownComponent existing, KnownComponent candidate) {
        boolean existingAnchored = looksAnchored(existing.cpq());
        boolean candidateAnchored = looksAnchored(candidate.cpq());
        if (candidateAnchored && !existingAnchored) {
            return true;
        }
        if (candidateAnchored == existingAnchored) {
            // Prefer more descriptive derivation (longer CPQ string) as a heuristic for richer structure.
            int existingLength = existing.cpqRule().length();
            int candidateLength = candidate.cpqRule().length();
            if (candidateLength > existingLength) {
                return true;
            }
        }
        return false;
    }

    private static final Comparator<KnownComponent> CANDIDATE_COMPARATOR =
            Comparator.comparingInt((KnownComponent kc) -> kc.edges().cardinality())
                    .thenComparing(KnownComponent::source)
                    .thenComparing(KnownComponent::target)
                    .thenComparing(kc -> kc.cpq().toString());

    private List<KnownComponent> dedupe(List<KnownComponent> candidates) {
        List<KnownComponent> deduped = new ArrayList<>(candidates.size());
        Set<String> seen = new LinkedHashSet<>();
        for (KnownComponent candidate : candidates) {
            String key = canonicalKey(candidate);
            if (seen.add(key)) {
                deduped.add(candidate);
            }
        }
        return deduped;
    }

    private boolean matchesComponent(KnownComponent candidate) {
        BitSet edgeBits = candidate.edges();
        List<Edge> componentEdges = edgesFor(edgeBits);
        if (componentEdges.isEmpty()) {
            return false;
        }

        LinkedHashSet<String> componentVertices = new LinkedHashSet<>();
        for (Edge edge : componentEdges) {
            componentVertices.add(edge.source());
            componentVertices.add(edge.target());
        }

        if (!componentVertices.contains(candidate.source()) || !componentVertices.contains(candidate.target())) {
            return false;
        }

        CQ cqPattern;
        QueryGraphCQ queryGraph;
        UniqueGraph<VarCQ, AtomCQ> graph;
        try {
            cqPattern = candidate.cpq().toCQ();
            queryGraph = cqPattern.toQueryGraph();
            graph = queryGraph.toUniqueGraph();
        } catch (RuntimeException ex) {
            return false;
        }

        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = graph.getEdges();
        if (cpqEdges.size() != componentEdges.size()) {
            return false;
        }

        QueryGraphCPQ cpqGraph = candidate.cpq().toQueryGraph();
        String sourceVarName = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
        String targetVarName = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());

        Map<String, String> variableMapping = new HashMap<>();
        Set<String> usedNodes = new HashSet<>();
        variableMapping.put(sourceVarName, candidate.source());
        variableMapping.put(targetVarName, candidate.target());
        usedNodes.add(candidate.source());
        usedNodes.add(candidate.target());

        return matchEdges(0, cpqEdges, new ArrayList<>(componentEdges), variableMapping, usedNodes,
                sourceVarName, targetVarName, candidate.source(), candidate.target());
    }

    private List<Edge> edgesFor(BitSet bits) {
        List<Edge> selected = new ArrayList<>(bits.cardinality());
        for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
            selected.add(edges.get(idx));
        }
        return selected;
    }

    private boolean matchEdges(int index,
                               List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
                               List<Edge> remaining,
                               Map<String, String> variableMapping,
                               Set<String> usedNodes,
                               String sourceVarName,
                               String targetVarName,
                               String expectedSource,
                               String expectedTarget) {
        if (index == cpqEdges.size()) {
            String mappedSource = variableMapping.get(sourceVarName);
            String mappedTarget = variableMapping.get(targetVarName);
            return remaining.isEmpty()
                    && expectedSource.equals(mappedSource)
                    && expectedTarget.equals(mappedTarget);
        }

        GraphEdge<VarCQ, AtomCQ> cpqEdge = cpqEdges.get(index);
        AtomCQ atom = cpqEdge.getData();
        String label = atom.getLabel().getAlias();
        String cpqSrcName = cpqEdge.getSourceNode().getData().getName();
        String cpqTrgName = cpqEdge.getTargetNode().getData().getName();

        for (int i = 0; i < remaining.size(); i++) {
            Edge edge = remaining.get(i);
            if (!label.equals(edge.label())) {
                continue;
            }
            String componentSource = edge.source();
            String componentTarget = edge.target();

            String mappedSrc = variableMapping.get(cpqSrcName);
            String mappedTrg = variableMapping.get(cpqTrgName);

            if (mappedSrc != null && !mappedSrc.equals(componentSource)) {
                continue;
            }
            if (mappedTrg != null && !mappedTrg.equals(componentTarget)) {
                continue;
            }
            if (mappedSrc == null && usedNodes.contains(componentSource)) {
                continue;
            }
            if (mappedTrg == null && usedNodes.contains(componentTarget)) {
                continue;
            }

            boolean addedSrc = false;
            boolean addedTrg = false;
            if (mappedSrc == null) {
                variableMapping.put(cpqSrcName, componentSource);
                usedNodes.add(componentSource);
                addedSrc = true;
            }
            if (mappedTrg == null) {
                variableMapping.put(cpqTrgName, componentTarget);
                usedNodes.add(componentTarget);
                addedTrg = true;
            }

            Edge removed = remaining.remove(i);
            if (matchEdges(index + 1, cpqEdges, remaining, variableMapping, usedNodes,
                    sourceVarName, targetVarName, expectedSource, expectedTarget)) {
                return true;
            }

            remaining.add(i, removed);
            if (addedSrc) {
                variableMapping.remove(cpqSrcName);
                usedNodes.remove(componentSource);
            }
            if (addedTrg) {
                variableMapping.remove(cpqTrgName);
                usedNodes.remove(componentTarget);
            }
        }

        return false;
    }

    private String canonicalKey(KnownComponent candidate) {
        String edgeSignature = BitsetUtils.signature(candidate.edges(), edges.size());
        String normalizedCPQ = normalizeCPQ(candidate.cpq().toString());
        return edgeSignature + "|" + candidate.source() + "|" + candidate.target() + "|" + normalizedCPQ;
    }

    /**
     * Normalizes a CPQ string representation to eliminate semantic duplicates.
     * Specifically handles conjunction commutativity: (a ∩ b) and (b ∩ a) become the same canonical form.
     * Preserves composition order since composition is not commutative.
     *
     * Algorithm:
     * 1. Parse the CPQ string into a tree structure
     * 2. Recursively normalize nested expressions
     * 3. For conjunction nodes, sort the operands alphabetically
     * 4. Reconstruct the normalized string
     */
    private String normalizeCPQ(String cpqStr) {
        // Remove all whitespace for consistent parsing
        String normalized = cpqStr.replaceAll("\\s+", "");

        // Recursively normalize the expression
        return normalizeExpression(normalized);
    }

    /**
     * Recursively normalizes a CPQ expression.
     * Handles nested parentheses and operators.
     */
    private String normalizeExpression(String expr) {
        // Base case: simple atom (no operators)
        if (!expr.contains("∩") && !expr.contains("◦")) {
            return expr;
        }

        // Handle parentheses
        if (expr.startsWith("(") && expr.endsWith(")")) {
            // Find the matching closing parenthesis
            int depth = 0;
            int matchingClose = -1;
            for (int i = 0; i < expr.length(); i++) {
                if (expr.charAt(i) == '(') depth++;
                else if (expr.charAt(i) == ')') {
                    depth--;
                    if (depth == 0) {
                        matchingClose = i;
                        break;
                    }
                }
            }

            // If the outer parentheses wrap the whole expression, remove them and recurse
            if (matchingClose == expr.length() - 1) {
                String inner = expr.substring(1, expr.length() - 1);
                return "(" + normalizeExpression(inner) + ")";
            }
        }

        // Find the top-level operator (not inside parentheses)
        int topLevelOp = findTopLevelOperator(expr);

        if (topLevelOp == -1) {
            // No operator at top level, just return as-is
            return expr;
        }

        char op = expr.charAt(topLevelOp);

        if (op == '∩') {
            // Conjunction: normalize and sort operands
            return normalizeConjunction(expr, topLevelOp);
        } else if (op == '◦') {
            // Composition: normalize operands but preserve order
            return normalizeComposition(expr, topLevelOp);
        }

        return expr;
    }

    /**
     * Finds the position of the top-level operator (not inside parentheses).
     * Returns -1 if no top-level operator is found.
     * Prioritizes finding ∩ first (lower precedence), then ◦.
     */
    private int findTopLevelOperator(String expr) {
        int depth = 0;

        // First pass: look for conjunction (∩)
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && c == '∩') {
                return i;
            }
        }

        // Second pass: look for composition (◦)
        depth = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && c == '◦') {
                return i;
            }
        }

        return -1;
    }

    /**
     * Normalizes a conjunction by sorting its operands.
     * Handles nested conjunctions by flattening them first.
     */
    private String normalizeConjunction(String expr, int opPos) {
        // Split by all top-level conjunctions
        List<String> operands = splitByTopLevelOperator(expr, '∩');

        // Normalize each operand
        List<String> normalizedOperands = new ArrayList<>();
        for (String operand : operands) {
            normalizedOperands.add(normalizeExpression(operand.trim()));
        }

        // Sort the operands alphabetically for canonical form
        normalizedOperands.sort(String::compareTo);

        // Reconstruct with parentheses around the whole expression
        if (normalizedOperands.size() == 1) {
            return normalizedOperands.get(0);
        }

        StringBuilder result = new StringBuilder("(");
        for (int i = 0; i < normalizedOperands.size(); i++) {
            if (i > 0) result.append("∩");
            result.append(normalizedOperands.get(i));
        }
        result.append(")");

        return result.toString();
    }

    /**
     * Normalizes a composition by normalizing operands but preserving order.
     */
    private String normalizeComposition(String expr, int opPos) {
        String left = expr.substring(0, opPos);
        String right = expr.substring(opPos + 1);

        String normalizedLeft = normalizeExpression(left.trim());
        String normalizedRight = normalizeExpression(right.trim());

        return "(" + normalizedLeft + "◦" + normalizedRight + ")";
    }

    /**
     * Splits an expression by a top-level operator (not inside parentheses).
     * Returns all operands separated by that operator at the top level.
     */
    private List<String> splitByTopLevelOperator(String expr, char operator) {
        List<String> operands = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && c == operator) {
                operands.add(expr.substring(start, i));
                start = i + 1;
            }
        }

        // Add the last operand
        if (start < expr.length()) {
            operands.add(expr.substring(start));
        }

        return operands;
    }
}
