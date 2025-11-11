package decomposition.partitions;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.Example;
import decomposition.cpq.CPQEngine;
import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.PartitionAnalysis;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.JoinNodeUtils;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class CPQEngineCacheTest {

  @Test
  void cacheStatsCaptureHitsAndMisses() {
    CQ query = Example.example1();
    CQExtractor extractor = new CQExtractor();
    ExtractionResult extraction = extractor.extract(query, Set.of());
    List<Edge> edges = extraction.edges();
    Map<String, String> varMap = extraction.variableNodeMap();

    PartitionGenerator generator = new PartitionGenerator(0);
    List<Component> components = generator.enumerateConnectedComponents(edges);
    List<Partition> partitions = generator.enumeratePartitions(edges, components);
    Partition partition = partitions.get(0);

    Set<String> joinNodes =
        JoinNodeUtils.computeJoinNodes(partition.components(), extraction.freeVariables());
    CacheStats stats = new CacheStats();
    CPQEngine engine = new CPQEngine(edges, stats);

    PartitionAnalysis first =
        engine.analyzePartition(partition, joinNodes, extraction.freeVariables(), varMap);
    PartitionAnalysis second =
        engine.analyzePartition(partition, joinNodes, extraction.freeVariables(), varMap);

    assertTrue(
        first != null && second != null, "Expected at least one component construction rule set");
    assertTrue(
        second.components().size() == first.components().size(),
        "Repeated lookups should return same count");

    CacheStats snapshot = stats.snapshot();
    assertTrue(snapshot.misses() > 0, "Initial population should record cache misses");
    assertTrue(snapshot.hits() > 0, "Repeated lookup should hit cache");
  }
}
