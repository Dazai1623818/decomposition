package decomposition.eval;

import decomposition.core.DecompositionResult;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.ProgressListener;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    CpqNativeIndex index = buildIndex(graphPath);
    LeapfrogCpqJoiner joiner = LeapfrogCpqJoiner.fromIndex(index);

    System.out.println("Loaded single-edge labels: " + joiner.singleEdgeLabels());
    System.out.println("Executing query: " + query.toFormalSyntax());
    List<Map<String, Integer>> baseline = joiner.executeBaseline(query);
    System.out.println("Single-edge result count: " + baseline.size());
    for (int i = 0; i < Math.min(10, baseline.size()); i++) {
      System.out.println(
          "  [baseline] " + DecompositionComparisonReporter.formatAssignment(baseline.get(i)));
    }

    JoinedDecompositionExecutor executor = new JoinedDecompositionExecutor(joiner);
    boolean executed = false;

    List<Partition> partitions = result.cpqPartitions();
    int skipped = 0;
    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      if (!isSupported(partition, query, result.edges(), joiner)) {
        skipped++;
        continue;
      }

      String label = "pipeline-partition-" + (i + 1);
      executor.setDebugLabel(label);
      List<Map<String, Integer>> joinedResults = executor.execute(partition, query, result.edges());
      System.out.println("Decomposition '" + label + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(label, baseline, joinedResults);
      executed = true;
    }

    if (skipped > 0) {
      System.out.println(
          "Skipped " + skipped + " partitions (components not fully supported by index k=3)");
    }

    List<PartitionDecompositionLoader.NamedDecomposition> external =
        loadExternalDecompositions(query);
    for (PartitionDecompositionLoader.NamedDecomposition task : external) {
      List<Map<String, Integer>> joinedResults = executor.execute(task.decomposition());
      System.out.println(
          "Decomposition '" + task.name() + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(task.name(), baseline, joinedResults);
      executed = true;
    }

    if (!executed) {
      System.out.println("No decompositions available for comparison.");
    }
  }

  private boolean isSupported(
      Partition partition, CQ query, List<Edge> edges, LeapfrogCpqJoiner joiner) {
    Set<String> freeVars = new HashSet<>();
    for (VarCQ v : query.getFreeVariables()) {
      freeVars.add("?" + v.getName());
    }

    for (decomposition.core.model.Component component : partition.components()) {
      List<AtomCQ> atoms = resolveAtoms(component, edges, query);
      if (!joiner.isIndexable(atoms)) {
        return false;
      }

      Set<String> internal = joiner.getInternalVariables(atoms);
      // Check 1: Internal vars cannot be free vars
      for (String v : internal) {
        if (freeVars.contains(v)) {
          return false;
        }
      }

      // Check 2: Internal vars cannot be in other components
      for (decomposition.core.model.Component other : partition.components()) {
        if (other.equals(component)) {
          continue;
        }
        List<AtomCQ> otherAtoms = resolveAtoms(other, edges, query);
        Set<String> otherVars = joiner.getAllVariables(otherAtoms);
        for (String v : internal) {
          if (otherVars.contains(v)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private List<AtomCQ> resolveAtoms(
      decomposition.core.model.Component component, List<Edge> edges, CQ query) {
    List<AtomCQ> atoms = new ArrayList<>();
    java.util.BitSet bits = component.edgeBits();
    // We need a way to map edges back to atoms.
    // Since we don't have a direct map here, we can reconstruct it or pass it.
    // For now, let's assume we can map by index if we preserved order,
    // but CQExtractor assigns synthetic IDs.
    // Actually, JoinedDecompositionExecutor has logic to resolve atoms.
    // Let's duplicate a simplified version or assume we can just check the edges.
    // But LeapfrogCpqJoiner needs AtomCQ objects to check signatures.

    // Re-using logic from JoinedDecompositionExecutor would be best, but it's
    // private.
    // Let's implement a quick resolver here.
    UniqueGraph<VarCQ, AtomCQ> graph = query.toQueryGraph().toUniqueGraph();
    List<AtomCQ> allAtoms = new ArrayList<>();
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      allAtoms.add(edge.getData());
    }

    // The edge list in DecompositionResult corresponds to these atoms?
    // CQExtractor iterates graph.getEdges() to build the edge list.
    // So the index in 'edges' should match the index in 'allAtoms' if iteration
    // order is deterministic.
    // LinkedHashMap/LinkedHashSet in CQExtractor ensures this.

    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      if (i < allAtoms.size()) {
        atoms.add(allAtoms.get(i));
      }
    }
    return atoms;
  }

  private CpqNativeIndex buildIndex(Path graphPath) throws IOException {
    try {
      UniqueGraph<Integer, Predicate> graph = GraphLoader.load(graphPath);
      CpqNativeIndex index =
          new CpqNativeIndex(
              graph,
              3,
              false,
              true,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
              -1,
              ProgressListener.none());
      index.sort();
      return index;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
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
}
