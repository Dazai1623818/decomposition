package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DecomposerTest {
    @Test
    void cpqkCoverExhaustiveRejectsNegativeK() {
        assertThrows(IllegalArgumentException.class, () -> Decomposer.cpqkCoverExhaustive(-1, 0));
    }

    @Test
    void cpqkCoverExhaustiveRejectsNegativeLimit() {
        assertThrows(IllegalArgumentException.class, () -> Decomposer.cpqkCoverExhaustive(1, -1));
    }

    @Test
    @Timeout(60)
    void randomCpqsRecoverMutuallyHomomorphicComponentFromDecompositions() {
        int queryCount = Integer.getInteger("cpq.test.randomCount", 3);
        int depth = Integer.getInteger("cpq.test.randomDepth", 5);
        int labelCount = Integer.getInteger("cpq.test.randomLabelCount", 4);
        int baseK = Integer.getInteger("cpq.test.randomK", 10);
        int limit = Integer.getInteger("cpq.test.randomLimit", 0);
        long baseSeed = 1;

        for (int i = 0; i < queryCount; i++) {
            long seed = baseSeed + i;
            Util.setRandomSeed(seed);

            CPQ cpq = CPQ.generateRandomCPQ(depth, labelCount);
            CQ cq = cpq.toCQ();
            int k = Math.max(baseK, cpq.getDiameter());

            Decomposer decomposer = Decomposer.cpqkCoverExhaustive(k, limit);
            boolean found = decomposer.decompose(cq).anyMatch(decomposition ->
                    decomposition.components().stream().anyMatch(component -> {
                        CPQ candidate = component.cpq();
                        return cpq.isHomomorphicTo(candidate) && candidate.isHomomorphicTo(cpq);
                    }));

            assertTrue(
                    found,
                    () -> "Missing mutually homomorphic component from decompositions"
                            + " (seed=" + seed
                            + ", k=" + k
                            + ", cpq=" + cpq.toFormalSyntax() + ")");
        }
    }
}
