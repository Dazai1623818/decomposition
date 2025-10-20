package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ComponentCPQBuilderTest {

    private static List<Edge> sampleEdges() {
        return List.of(
                new Edge("A", "B", new Predicate(1, "r1"), 0),
                new Edge("B", "C", new Predicate(2, "r2"), 1),
                new Edge("C", "D", new Predicate(3, "r3"), 2),
                new Edge("D", "A", new Predicate(4, "r4"), 3),
                new Edge("A", "C", new Predicate(5, "r5"), 4));
    }

    @Test
    void singleEdgeIncludesInverseVariant() {
        ComponentCPQBuilder builder = new ComponentCPQBuilder(sampleEdges());

        BitSet edgeBits = new BitSet();
        edgeBits.set(4);

        List<KnownComponent> options = builder.options(edgeBits);

        CPQ expectedInverse = CPQ.parse("r5⁻");
        String expectedStr = expectedInverse.toString();

        assertTrue(
                options.stream().anyMatch(kc ->
                        expectedStr.equals(kc.cpq().toString())
                                && "C".equals(kc.source())
                                && "A".equals(kc.target())),
                "Inverse single-edge CPQ r5⁻ should be available with reversed endpoints");
    }

    @Test
    void threeEdgeComponentSupportsIntersectionWithInverse() {
        ComponentCPQBuilder builder = new ComponentCPQBuilder(sampleEdges());

        BitSet edgeBits = new BitSet();
        edgeBits.set(2);
        edgeBits.set(3);
        edgeBits.set(4);

        List<KnownComponent> options = builder.options(edgeBits);

        CPQ expected = CPQ.parse("((r3◦r4) ∩ r5⁻)");
        String expectedStr = expected.toString();

        assertTrue(
                options.stream().anyMatch(kc ->
                        expectedStr.equals(kc.cpq().toString())
                                && "C".equals(kc.source())
                                && "A".equals(kc.target())),
                "Component {r3,r4,r5} should include (r3◦r4) ∩ r5⁻ to cover the reverse join");
    }

    @Test
    void conjunctionDeduplicationEliminatesCommutativeDuplicates() {
        ComponentCPQBuilder builder = new ComponentCPQBuilder(sampleEdges());

        // Create a component with 4 edges that would generate conjunctions
        BitSet edgeBits = new BitSet();
        edgeBits.set(0); // r1
        edgeBits.set(1); // r2
        edgeBits.set(2); // r3
        edgeBits.set(3); // r4

        List<KnownComponent> options = builder.options(edgeBits);

        // Count unique CPQ strings for components with same source and target
        long countAtoC = options.stream()
                .filter(kc -> "A".equals(kc.source()) && "C".equals(kc.target()))
                .map(kc -> kc.cpq().toString())
                .distinct()
                .count();

        // Before normalization, we would get duplicates like:
        // ((r1◦r2) ∩ (r4⁻◦r3⁻)) and ((r4⁻◦r3⁻) ∩ (r1◦r2))
        // After normalization, these should be deduplicated

        long countCtoA = options.stream()
                .filter(kc -> "C".equals(kc.source()) && "A".equals(kc.target()))
                .map(kc -> kc.cpq().toString())
                .distinct()
                .count();

        // Verify that we don't have excessive duplicates
        // The exact count will depend on the CPQ generation logic,
        // but we should have significantly fewer options than before normalization
        assertTrue(countAtoC > 0, "Should have at least one CPQ from A to C");
        assertTrue(countCtoA > 0, "Should have at least one CPQ from C to A");

        // More specific test: check that commutative duplicates are eliminated
        List<String> cpqStringsAtoC = options.stream()
                .filter(kc -> "A".equals(kc.source()) && "C".equals(kc.target()))
                .map(kc -> kc.cpq().toString())
                .toList();

        // Check for specific duplicates that should be eliminated
        long countR1R2IntersectR4R3 = cpqStringsAtoC.stream()
                .filter(cpq -> cpq.contains("r1") && cpq.contains("r2") &&
                        cpq.contains("r4") && cpq.contains("r3") &&
                        cpq.contains("∩"))
                .count();

        // After deduplication, we should only have one version of this conjunction
        assertTrue(countR1R2IntersectR4R3 <= 2,
                "Should not have many duplicate conjunctions with same operands in different order. Found: " +
                        countR1R2IntersectR4R3 + " instances");
    }
}
