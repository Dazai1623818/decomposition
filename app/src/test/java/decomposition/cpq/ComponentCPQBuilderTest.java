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
}
