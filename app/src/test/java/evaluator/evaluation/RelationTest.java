package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import evaluator.evaluation.Relation.RelationProjection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RelationTest {
    @Test
    void unaryBindingsReturnDomain() {
        Relation binding = Relation.unary("?x", "u", new int[] { 1, 2 });
        int[] domain = binding.domainFor("?x", new int[1], new boolean[1], Map.of("?x", 0));
        assertArrayEquals(new int[] { 1, 2 }, domain);
    }

    @Test
    void binaryBindingsRespectBoundVariables() {
        Map<Integer, int[]> forward = new HashMap<>();
        forward.put(1, new int[] { 3 });
        forward.put(2, new int[] { 4 });
        Map<Integer, int[]> reverse = new HashMap<>();
        reverse.put(3, new int[] { 1 });
        reverse.put(4, new int[] { 2 });

        RelationProjection projection = new RelationProjection(
                new int[] { 1, 2 },
                new int[] { 3, 4 },
                forward,
                reverse);
        Relation binding = Relation.binary("?x", "?y", "r", projection);

        int[] assignment = new int[2];
        boolean[] bound = new boolean[2];
        Map<String, Integer> indexByVar = Map.of("?x", 0, "?y", 1);

        assertArrayEquals(new int[] { 1, 2 }, binding.domainFor("?x", assignment, bound, indexByVar));

        assignment[1] = 3;
        bound[1] = true;
        assertArrayEquals(new int[] { 1 }, binding.domainFor("?x", assignment, bound, indexByVar));
    }

    @Test
    void domainForRejectsUnknownVariable() {
        Map<Integer, int[]> forward = new HashMap<>();
        forward.put(1, new int[] { 2 });
        Map<Integer, int[]> reverse = new HashMap<>();
        reverse.put(2, new int[] { 1 });
        RelationProjection projection = new RelationProjection(
                new int[] { 1 },
                new int[] { 2 },
                forward,
                reverse);
        Relation binding = Relation.binary("?x", "?y", "r", projection);

        assertThrows(IllegalArgumentException.class, () -> binding.domainFor(
                "?z",
                new int[2],
                new boolean[2],
                Map.of("?x", 0, "?y", 1)));
    }
}
