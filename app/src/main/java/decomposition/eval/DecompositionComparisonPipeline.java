package decomposition.eval;

import decomposition.DecompositionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.core.graph.Predicate;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DecompositionComparisonPipeline {
  private final Path graphPath;
  private final Path nativeLibrary;
  private final List<Path> decompositionInputs;

  public DecompositionComparisonPipeline(
      Path graphPath, Path nativeLibrary, List<Path> decompositionInputs) {
    this.graphPath = Objects.requireNonNull(graphPath, "graphPath");
    this.nativeLibrary = Objects.requireNonNull(nativeLibrary, "nativeLibrary");
    this.decompositionInputs =
        decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
  }

  public void evaluate(CQ query, DecompositionResult result) throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");

    System.load(nativeLibrary.toAbsolutePath().toString());
    Index index = buildIndex(graphPath);
    LeapfrogEdgeJoiner joiner = LeapfrogEdgeJoiner.fromIndex(index);

    System.out.println("Loaded labels: " + joiner.labels());
    System.out.println("Executing query: " + query.toFormalSyntax());
    List<Map<String, Integer>> baseline = joiner.execute(query);
    System.out.println("Single-edge result count: " + baseline.size());

    List<PartitionDecompositionLoader.NamedDecomposition> decompositions =
        collectDecompositions(query, result);
    if (decompositions.isEmpty()) {
      System.out.println("No decompositions available for comparison.");
      return;
    }

    JoinedDecompositionExecutor executor = new JoinedDecompositionExecutor(joiner);
    for (PartitionDecompositionLoader.NamedDecomposition task : decompositions) {
      List<Map<String, Integer>> joinedResults = executor.execute(task.decomposition());
      System.out.println(
          "Decomposition '" + task.name() + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(task.name(), baseline, joinedResults);
    }
  }

  private Index buildIndex(Path graphPath) throws IOException {
    try {
      UniqueGraph<Integer, Predicate> graph = GraphLoader.load(graphPath);
      Index index =
          new Index(
              graph,
              1,
              false,
              true,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
              -1,
              ProgressListener.NONE);
      index.sort();
      return index;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
  }

  private List<PartitionDecompositionLoader.NamedDecomposition> collectDecompositions(
      CQ query, DecompositionResult result) throws IOException {
    List<PartitionDecompositionLoader.NamedDecomposition> tasks = new ArrayList<>();
    tasks.addAll(buildPipelineDecompositions(query, result));
    tasks.addAll(loadExternalDecompositions(query));
    return tasks;
  }

  private List<PartitionDecompositionLoader.NamedDecomposition> buildPipelineDecompositions(
      CQ query, DecompositionResult result) {
    List<PartitionDecompositionLoader.NamedDecomposition> tasks = new ArrayList<>();
    if (result.cpqPartitions().isEmpty()) {
      return tasks;
    }
    PartitionToQueryDecomposition converter =
        new PartitionToQueryDecomposition(query, result.edges());
    List<Partition> partitions = result.cpqPartitions();
    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      QueryDecomposition decomposition = converter.convert(partition);
      String label = "pipeline-partition-" + (i + 1);
      tasks.add(new PartitionDecompositionLoader.NamedDecomposition(label, decomposition));
    }
    return tasks;
  }

  private List<PartitionDecompositionLoader.NamedDecomposition> loadExternalDecompositions(CQ query)
      throws IOException {
    List<PartitionDecompositionLoader.NamedDecomposition> tasks = new ArrayList<>();
    if (decompositionInputs.isEmpty()) {
      return tasks;
    }
    PartitionDecompositionLoader loader = PartitionDecompositionLoader.forQuery(query);
    for (Path input : decompositionInputs) {
      List<PartitionDecompositionLoader.NamedDecomposition> loaded = loader.load(input);
      if (loaded.isEmpty()) {
        System.out.println("No decompositions found in " + input);
        continue;
      }
      boolean directory = Files.isDirectory(input);
      for (PartitionDecompositionLoader.NamedDecomposition named : loaded) {
        String label = directory ? input + "/" + named.name() : input.toString();
        tasks.add(
            new PartitionDecompositionLoader.NamedDecomposition(label, named.decomposition()));
      }
    }
    return tasks;
  }

  private static String signature(Edge edge) {
    return signature(edge.source(), edge.label(), edge.target());
  }

  private static String signature(String source, String label, String target) {
    return source + "|" + label + "|" + target;
  }

  private static final class PartitionToQueryDecomposition {
    private final List<Edge> edges;
    private final Map<String, List<AtomCQ>> lookup;

    PartitionToQueryDecomposition(CQ query, List<Edge> edges) {
      this.edges = List.copyOf(edges);
      this.lookup = indexAtoms(query);
    }

    QueryDecomposition convert(Partition partition) {
      Map<String, Integer> usage = new HashMap<>();
      List<List<AtomCQ>> atomsPerComponent = new ArrayList<>();
      for (Component component : partition.components()) {
        List<AtomCQ> componentAtoms = new ArrayList<>();
        BitSet bits = component.edgeBits();
        for (int edgeIndex = bits.nextSetBit(0);
            edgeIndex >= 0;
            edgeIndex = bits.nextSetBit(edgeIndex + 1)) {
          componentAtoms.add(resolve(edgeIndex, usage));
        }
        atomsPerComponent.add(componentAtoms);
      }
      if (atomsPerComponent.isEmpty()) {
        throw new IllegalStateException("Partition contains no components");
      }
      if (atomsPerComponent.size() == 1) {
        return QueryDecomposition.of(QueryDecomposition.bag(atomsPerComponent.get(0)));
      }
      List<QueryDecomposition.Bag> children = new ArrayList<>();
      for (int i = 1; i < atomsPerComponent.size(); i++) {
        children.add(QueryDecomposition.bag(atomsPerComponent.get(i)));
      }
      return QueryDecomposition.of(QueryDecomposition.bag(atomsPerComponent.get(0), children));
    }

    private AtomCQ resolve(int edgeIndex, Map<String, Integer> usage) {
      Edge edge = edges.get(edgeIndex);
      String key = DecompositionComparisonPipeline.signature(edge);
      List<AtomCQ> candidates = lookup.get(key);
      if (candidates == null || candidates.isEmpty()) {
        throw new IllegalStateException("No atoms available for edge " + key);
      }
      int count = usage.getOrDefault(key, 0);
      if (count >= candidates.size()) {
        throw new IllegalStateException("Not enough atoms for edge " + key);
      }
      usage.put(key, count + 1);
      return candidates.get(count);
    }

    private static Map<String, List<AtomCQ>> indexAtoms(CQ query) {
      dev.roanh.gmark.util.graph.generic.UniqueGraph<VarCQ, AtomCQ> graph =
          query.toQueryGraph().toUniqueGraph();
      Map<String, List<AtomCQ>> index = new HashMap<>();
      for (dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge<VarCQ, AtomCQ> graphEdge :
          graph.getEdges()) {
        AtomCQ atom = graphEdge.getData();
        index.computeIfAbsent(signature(atom), ignored -> new ArrayList<>()).add(atom);
      }
      return index;
    }

    private static String signature(AtomCQ atom) {
      return DecompositionComparisonPipeline.signature(
          atom.getSource().getName(), atom.getLabel().getAlias(), atom.getTarget().getName());
    }
  }
}
