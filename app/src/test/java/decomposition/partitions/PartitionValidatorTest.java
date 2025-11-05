package decomposition.partitions;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.Example;
import decomposition.cpq.ComponentCPQBuilder;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionValidator.ComponentConstructionRules;
import decomposition.partitions.PartitionValidator.ComponentRuleCacheStats;
import decomposition.partitions.PartitionValidator.ComponentRuleCacheStats.CacheSnapshot;
import decomposition.util.JoinNodeUtils;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class PartitionValidatorTest {

  @Test
  void cacheStatsCaptureHitsAndMisses() {
    CQ query = Example.example1();
    CQExtractor extractor = new CQExtractor();
    ExtractionResult extraction = extractor.extract(query, Set.of());
    List<Edge> edges = extraction.edges();

    PartitionGenerator generator = new PartitionGenerator(0);
    List<Component> components = generator.enumerateConnectedComponents(edges);
    List<Partition> partitions = generator.enumeratePartitions(edges, components);
    Partition partition = partitions.get(0);

    Set<String> joinNodes =
        JoinNodeUtils.computeJoinNodes(partition.components(), extraction.freeVariables());
    ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);
    ComponentRuleCacheStats stats = new ComponentRuleCacheStats();
    PartitionValidator validator = new PartitionValidator(stats);

    List<ComponentConstructionRules> first =
        validator.componentConstructionRules(
            partition, joinNodes, builder, extraction.freeVariables(), edges);
    List<ComponentConstructionRules> second =
        validator.componentConstructionRules(
            partition, joinNodes, builder, extraction.freeVariables(), edges);

    assertTrue(!first.isEmpty(), "Expected at least one component construction rule set");
    assertTrue(second.size() == first.size(), "Repeated lookups should return same count");

    CacheSnapshot snapshot = stats.snapshot();
    assertTrue(snapshot.misses() > 0, "Initial population should record cache misses");
    assertTrue(snapshot.hits() > 0, "Repeated lookup should hit cache");
  }
}
