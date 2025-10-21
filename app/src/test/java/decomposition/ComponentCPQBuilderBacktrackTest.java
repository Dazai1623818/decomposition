package decomposition;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Edge;
import decomposition.partitions.PartitionGenerator;
import decomposition.partitions.PartitionValidator;
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
        List<KnownComponent> anchoredAtJoin = options.stream()
                .filter(kc -> kc.derivation().startsWith("Single backtrack anchored at")
                        && "D".equals(kc.source())
                        && "D".equals(kc.target()))
                .collect(Collectors.toList());

        assertFalse(options.isEmpty(), "Expected CPQ options for single edge component");
        assertFalse(anchoredAtJoin.isEmpty(),
                () -> "Expected anchored backtrack variant for join node but saw:\n"
                        + options.stream()
                                .map(kc -> kc.cpqRule() + " [" + kc.source() + "→" + kc.target() + "] :: "
                                        + kc.derivation())
                                .collect(Collectors.joining("\n")));

        PartitionGenerator generator = new PartitionGenerator(0);
        List<decomposition.model.Component> components = generator.enumerateConnectedComponents(edges);
        List<decomposition.model.Partition> partitions = generator.enumeratePartitions(edges, components);

        decomposition.model.Partition singleEdgePartition = partitions.stream()
                .filter(p -> p.components().stream().allMatch(component -> component.edgeCount() == 1))
                .findFirst()
                .orElseThrow();

        PartitionValidator validator = new PartitionValidator();
        assertTrue(validator.isValidCPQDecomposition(singleEdgePartition, builder, extraction.freeVariables(), edges),
                "Single-edge partition should now be a valid CPQ decomposition");
    }

    @Test
    void selfLoopComponentsExposeAnchoredVariant() {
        CQ cq = Example.example7();
        CQExtractor extractor = new CQExtractor();
        ExtractionResult extraction = extractor.extract(cq, Set.of("A"));
        List<Edge> edges = extraction.edges();

        ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);

        int selfLoopIndex = findSelfLoopEdge(edges);
        BitSet selfLoopBits = new BitSet(edges.size());
        selfLoopBits.set(selfLoopIndex);

        List<KnownComponent> options = builder.options(selfLoopBits);

        assertTrue(options.stream().anyMatch(kc -> kc.cpqRule().contains("∩ id")),
                () -> "Expected anchored variant for self-loop but saw: "
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
