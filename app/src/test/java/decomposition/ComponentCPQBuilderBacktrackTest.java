package decomposition;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Edge;
import decomposition.partitions.PartitionGenerator;
import decomposition.partitions.PartitionValidator;
import decomposition.util.JoinNodeUtils;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class ComponentCPQBuilderBacktrackTest {

    @Test
    void singleEdgeBacktrackVariantsAreGeneratedAndUsable() {
        CQ cq = Example.example5();
        CQExtractor extractor = new CQExtractor();
        ExtractionResult extraction = extractor.extract(cq, Set.of("D"));
        List<Edge> edges = extraction.edges();

        ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);

        int r4Index = findEdgeIndex(edges, "r4");
        BitSet r4Bits = new BitSet(edges.size());
        r4Bits.set(r4Index);

        assertDoesNotThrow(() -> dev.roanh.gmark.lang.cpq.CPQ.parse("(r4 ◦ r4⁻) ∩ id"));
        assertDoesNotThrow(() -> dev.roanh.gmark.lang.cpq.CPQ.parse("(r4⁻ ◦ r4) ∩ id"));

        List<KnownComponent> options = builder.options(r4Bits);
        Edge r4 = edges.get(r4Index);

        assertFalse(options.isEmpty(), "Expected CPQ options for single edge component");
        assertTrue(options.stream()
                        .filter(kc -> kc.source().equals(r4.source())
                                && kc.target().equals(r4.source()))
                        .allMatch(kc -> kc.cpqRule().contains("∩ id")),
                () -> "Backtrack loop at source missing ∩ id but saw:\n" + options.stream()
                        .map(kc -> kc.cpqRule() + " [" + kc.source() + "→" + kc.target() + "] :: "
                                + kc.derivation())
                        .collect(Collectors.joining("\n")));
        assertTrue(options.stream()
                        .filter(kc -> kc.source().equals(r4.target())
                                && kc.target().equals(r4.target()))
                        .allMatch(kc -> kc.cpqRule().contains("∩ id")),
                () -> "Backtrack loop at target missing ∩ id but saw:\n" + options.stream()
                        .map(kc -> kc.cpqRule() + " [" + kc.source() + "→" + kc.target() + "] :: "
                                + kc.derivation())
                        .collect(Collectors.joining("\n")));

        Set<List<String>> endpointPairs = options.stream()
                .map(kc -> List.of(kc.source(), kc.target()))
                .collect(Collectors.toSet());

        Set<List<String>> expectedPairs = Set.of(
                List.of(r4.source(), r4.target()),
                List.of(r4.target(), r4.source()),
                List.of(r4.source(), r4.source()),
                List.of(r4.target(), r4.target()));

        assertEquals(expectedPairs, endpointPairs,
                () -> "Expected forward, inverse, and backtrack orientations but saw: " + endpointPairs);

        PartitionGenerator generator = new PartitionGenerator(0);
        List<decomposition.model.Component> components = generator.enumerateConnectedComponents(edges);
        List<decomposition.model.Partition> partitions = generator.enumeratePartitions(edges, components);

        decomposition.model.Partition singleEdgePartition = partitions.stream()
                .filter(p -> p.components().stream().allMatch(component -> component.edgeCount() == 1))
                .findFirst()
                .orElseThrow();

        PartitionValidator validator = new PartitionValidator();
        Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(singleEdgePartition.components(), extraction.freeVariables());
        assertTrue(validator.isValidCPQDecomposition(singleEdgePartition, joinNodes, builder, extraction.freeVariables(), extraction.freeVariableOrder(), edges),
                "Single-edge partition should now be a valid CPQ decomposition");
    }

    @Test
    void selfLoopComponentsYieldSingleStructuralOption() {
        CQ cq = Example.example7();
        CQExtractor extractor = new CQExtractor();
        ExtractionResult extraction = extractor.extract(cq, Set.of("A"));
        List<Edge> edges = extraction.edges();

        ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);

        int selfLoopIndex = findSelfLoopEdge(edges);
        BitSet selfLoopBits = new BitSet(edges.size());
        selfLoopBits.set(selfLoopIndex);

        List<KnownComponent> options = builder.options(selfLoopBits);

        assertEquals(1, options.size(), "Self-loop should produce a single structural option");
        KnownComponent loop = options.get(0);
        assertTrue(loop.cpqRule().contains("∩ id"),
                () -> "Self-loop should enforce equality via id but saw: "
                        + options.stream()
                                .map(kc -> kc.cpqRule() + " [" + kc.source() + "→" + kc.target() + "]")
                                .collect(Collectors.joining(", ")));
    }

    private int findEdgeIndex(List<Edge> edges, String label) {
        for (int i = 0; i < edges.size(); i++) {
            if (label.equals(edges.get(i).label())) {
                return i;
            }
        }
        throw new IllegalArgumentException("Edge with label '" + label + "' not found");
    }

    private int findSelfLoopEdge(List<Edge> edges) {
        for (int i = 0; i < edges.size(); i++) {
            Edge edge = edges.get(i);
            if (edge.source().equals(edge.target())) {
                return i;
            }
        }
        throw new IllegalArgumentException("No self-loop edge found");
    }
}
