package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OrderCandidatesTest {
    @Test
    void smallQueriesEnumerateAllProjectedFirstOrders() {
        List<List<String>> candidates = OrderCandidates.build(
                List.of("?a", "?b", "?x", "?c"),
                List.of("?x"));

        assertEquals(6, candidates.size());
        assertEquals(6, new HashSet<>(candidates).size());
        for (List<String> candidate : candidates) {
            assertEquals("?x", candidate.get(0));
        }
    }

    @Test
    void smallQueriesWithoutProjectedVariablesEnumerateAllPermutations() {
        List<List<String>> candidates = OrderCandidates.build(
                List.of("?a", "?b", "?c", "?d"),
                List.of());

        assertEquals(24, candidates.size());
        assertEquals(24, new HashSet<>(candidates).size());
    }

    @Test
    void smallQueryBudgetKeepsBestProjectedFirstOrders() {
        List<List<String>> candidates = OrderCandidates.build(
                List.of("?a", "?b", "?x", "?c"),
                List.of("?x"),
                2);

        assertEquals(2, candidates.size());
        assertEquals(List.of("?x", "?a", "?b", "?c"), candidates.get(0));
        for (List<String> candidate : candidates) {
            assertEquals("?x", candidate.get(0));
        }
    }

    @Test
    void largerQueriesStayPrunedButRetainProjectedFirstCandidate() {
        List<String> base = List.of("?a", "?b", "?c", "?d", "?e", "?x", "?y");
        List<List<String>> candidates = OrderCandidates.build(base, List.of("?x", "?y"));
        Set<List<String>> unique = new HashSet<>(candidates);

        assertEquals(unique.size(), candidates.size());
        assertTrue(candidates.size() < 30);
        assertTrue(candidates.contains(base));
        assertTrue(candidates.contains(List.of("?x", "?y", "?a", "?b", "?c", "?d", "?e")));
        for (List<String> candidate : candidates) {
            assertEquals(base.size(), candidate.size());
            assertEquals(Set.copyOf(base), Set.copyOf(candidate));
        }
    }

    @Test
    void largerQueryBudgetTruncatesRankedCandidates() {
        List<String> base = List.of("?a", "?b", "?c", "?d", "?e", "?x", "?y");
        List<List<String>> candidates = OrderCandidates.build(base, List.of("?x", "?y"), 2);

        assertEquals(2, candidates.size());
        assertEquals(List.of("?x", "?y", "?a", "?b", "?c", "?d", "?e"), candidates.get(0));
        for (List<String> candidate : candidates) {
            assertEquals(base.size(), candidate.size());
            assertEquals(Set.copyOf(base), Set.copyOf(candidate));
        }
    }
}
