package decomposition.partitions;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.Example;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.JoinAnalysis;
import decomposition.util.JoinAnalysisBuilder;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

final class CPQEnumeratorCacheTest {

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

    JoinAnalysis analysis =
        JoinAnalysisBuilder.analyzePartition(partition, extraction.freeVariables());
    FilteredPartition filteredPartition = new FilteredPartition(partition, analysis);
    CacheStats stats = new CacheStats();
    PartitionDiagnostics diagnostics = new PartitionDiagnostics();
    PartitionExpressionAssembler synthesizer = new PartitionExpressionAssembler(edges);
    Map<ComponentCacheKey, CachedComponentExpressions> componentCache = new ConcurrentHashMap<>();

    List<List<CPQExpression>> first =
        synthesizer.synthesize(
            filteredPartition,
            extraction.freeVariables(),
            varMap,
            componentCache,
            stats,
            diagnostics,
            1);
    List<List<CPQExpression>> second =
        synthesizer.synthesize(
            filteredPartition,
            extraction.freeVariables(),
            varMap,
            componentCache,
            stats,
            diagnostics,
            1);

    assertTrue(first != null && second != null, "Expected at least one component expression set");
    assertTrue(second.size() == first.size(), "Repeated lookups should return same count");

    CacheStats snapshot = stats.snapshot();
    assertTrue(snapshot.misses() > 0, "Initial population should record cache misses");
    assertTrue(snapshot.hits() > 0, "Repeated lookup should hit cache");
  }
}
