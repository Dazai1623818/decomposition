package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OrderCandidatesTest {
    @Test
    void buildUsesLocalSingleInsertionNeighborhood() {
        List<String> base = List.of("?a", "?b", "?c", "?d");

        List<List<String>> candidates = OrderCandidates.build(base, List.of("?d"));

        assertTrue(candidates.contains(base));
        assertTrue(candidates.contains(List.of("?b", "?a", "?c", "?d")));
        assertTrue(candidates.contains(List.of("?a", "?c", "?b", "?d")));
        assertTrue(candidates.contains(List.of("?d", "?a", "?b", "?c")));
        assertTrue(!candidates.contains(List.of("?c", "?d", "?a", "?b")));
        for (List<String> candidate : candidates) {
            assertEquals(base.size(), candidate.size());
            assertEquals(Set.copyOf(base), Set.copyOf(candidate));
        }
    }

    @Test
    void buildDoesNotForceProjectedFirstOrders() {
        List<String> base = List.of("?a", "?b", "?c", "?d", "?e", "?x", "?y");

        List<List<String>> candidates = OrderCandidates.build(base, List.of("?x", "?y"));

        assertTrue(candidates.contains(base));
        assertTrue(candidates.contains(List.of("?x", "?a", "?b", "?c", "?d", "?e", "?y")));
        assertTrue(!candidates.contains(List.of("?x", "?y", "?a", "?b", "?c", "?d", "?e")));
    }

    @Test
    void localBudgetTruncatesDeterministicCandidateOrder() {
        List<String> base = List.of("?a", "?b", "?c");

        List<List<String>> candidates = OrderCandidates.buildLocal(base, List.of("?c"), 2);

        assertEquals(2, candidates.size());
        assertEquals(base, candidates.get(0));
        assertEquals(List.of("?a", "?c", "?b"), candidates.get(1));
    }
}
