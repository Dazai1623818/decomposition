package decomposition.eval;

import decomposition.core.model.*;
import dev.roanh.gmark.lang.cq.*;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Evaluates bags via {@link LeapfrogCpqJoiner} and joins their assignments. */
final class JoinedDecompositionExecutor {
  private final LeapfrogCpqJoiner joiner;
  private String debugLabel;

  JoinedDecompositionExecutor(LeapfrogCpqJoiner joiner) {
    this.joiner = Objects.requireNonNull(joiner, "joiner");
  }

  void setDebugLabel(String label) {
    this.debugLabel = label;
  }

  List<Map<String, Integer>> execute(QueryDecomposition decomposition) {
    Objects.requireNonNull(decomposition, "decomposition");
    return evaluate(decomposition.root());
  }

  public List<Map<String, Integer>> execute(Partition partition, CQ query, List<Edge> edges) {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(edges, "edges");
    QueryDecomposition.Bag root = buildExecutionPlan(partition, query, edges);
    return evaluate(root);
  }

  private QueryDecomposition.Bag buildExecutionPlan(
      Partition partition, CQ query, List<Edge> edges) {
    Map<String, List<AtomCQ>> atomsBySignature = indexAtoms(query);
    Map<String, Integer> usage = new HashMap<>();
    List<List<AtomCQ>> atomsPerComponent = new ArrayList<>();
    for (Component component : partition.components()) {
      List<AtomCQ> componentAtoms = new ArrayList<>();
      BitSet bits = component.edgeBits();
      for (int edgeIndex = bits.nextSetBit(0);
          edgeIndex >= 0;
          edgeIndex = bits.nextSetBit(edgeIndex + 1)) {
        componentAtoms.add(resolveAtom(edgeIndex, edges, atomsBySignature, usage));
      }
      atomsPerComponent.add(componentAtoms);
    }
    if (atomsPerComponent.isEmpty()) {
      throw new IllegalStateException("Partition contains no components");
    }
    if (atomsPerComponent.size() == 1) {
      return QueryDecomposition.bag(atomsPerComponent.get(0));
    }
    List<QueryDecomposition.Bag> children = new ArrayList<>();
    for (int i = 1; i < atomsPerComponent.size(); i++) {
      children.add(QueryDecomposition.bag(atomsPerComponent.get(i)));
    }
    return QueryDecomposition.bag(atomsPerComponent.get(0), children);
  }

  private static AtomCQ resolveAtom(
      int edgeIndex,
      List<Edge> edges,
      Map<String, List<AtomCQ>> atomsBySignature,
      Map<String, Integer> usage) {
    if (edgeIndex < 0 || edgeIndex >= edges.size()) {
      throw new IllegalArgumentException("Invalid edge index " + edgeIndex);
    }
    Edge edge = edges.get(edgeIndex);
    String key = signature(edge);
    List<AtomCQ> candidates = atomsBySignature.get(key);
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
    UniqueGraph<VarCQ, AtomCQ> graph = query.toQueryGraph().toUniqueGraph();
    Map<String, List<AtomCQ>> index = new HashMap<>();
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> graphEdge : graph.getEdges()) {
      AtomCQ atom = graphEdge.getData();
      index.computeIfAbsent(signature(atom), ignored -> new ArrayList<>()).add(atom);
    }
    return index;
  }

  private static String signature(Edge edge) {
    return signature(edge.source(), edge.label(), edge.target());
  }

  private static String signature(AtomCQ atom) {
    return signature(
        atom.getSource().getName(), atom.getLabel().getAlias(), atom.getTarget().getName());
  }

  private static String signature(String source, String label, String target) {
    return source + "|" + label + "|" + target;
  }

  private List<Map<String, Integer>> evaluate(QueryDecomposition.Bag bag) {
    List<Map<String, Integer>> bagResults = joiner.executeOptimized(bag.atoms());
    if ("pipeline-partition-29".equals(debugLabel)) {
      System.out.println(
          "[DEBUG "
              + debugLabel
              + "] bag atoms="
              + describeAtoms(bag.atoms())
              + " results="
              + bagResults.size());
      if (bag.atoms().size() > 1) {
        bagResults.forEach(
            assignment ->
                System.out.println(
                    "[DEBUG "
                        + debugLabel
                        + "]   "
                        + DecompositionComparisonReporter.formatAssignment(assignment)));
      } else {
        var seen = new java.util.LinkedHashSet<Integer>();
        for (Map<String, Integer> assignment : bagResults) {
          Integer d = assignment.get("?D");
          if (d != null) {
            seen.add(d);
          }
          if (seen.size() >= 20) {
            break;
          }
        }
        boolean hasD3 =
            bagResults.stream()
                .map(m -> m.get("?D"))
                .filter(java.util.Objects::nonNull)
                .anyMatch(d -> d == 3);
        System.out.println(
            "[DEBUG " + debugLabel + "]   D values sample=" + seen + " hasD3=" + hasD3);
        bagResults.stream()
            .limit(5)
            .forEach(
                assignment ->
                    System.out.println(
                        "[DEBUG "
                            + debugLabel
                            + "]   sample="
                            + DecompositionComparisonReporter.formatAssignment(assignment)));
      }
    }
    for (QueryDecomposition.Bag child : bag.children()) {
      List<Map<String, Integer>> childResults = evaluate(child);
      bagResults = naturalJoin(bagResults, childResults);
      if (bagResults.isEmpty()) {
        break;
      }
    }
    return bagResults;
  }

  private static String describeAtoms(List<AtomCQ> atoms) {
    List<String> desc = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      desc.add(
          atom.getLabel().getAlias()
              + "("
              + "?"
              + atom.getSource().getName()
              + "→"
              + "?"
              + atom.getTarget().getName()
              + ")");
    }
    return String.join(", ", desc);
  }

  private static List<Map<String, Integer>> naturalJoin(
      List<Map<String, Integer>> left, List<Map<String, Integer>> right) {
    if (left.isEmpty() || right.isEmpty()) {
      return List.of();
    }
    List<Map<String, Integer>> result = new ArrayList<>();
    for (Map<String, Integer> leftRow : left) {
      for (Map<String, Integer> rightRow : right) {
        if (!compatible(leftRow, rightRow)) {
          continue;
        }
        Map<String, Integer> joined = new LinkedHashMap<>(leftRow);
        for (Map.Entry<String, Integer> entry : rightRow.entrySet()) {
          joined.put(entry.getKey(), entry.getValue());
        }
        result.add(joined);
      }
    }
    return result;
  }

  private static boolean compatible(Map<String, Integer> left, Map<String, Integer> right) {
    for (Map.Entry<String, Integer> entry : left.entrySet()) {
      Integer other = right.get(entry.getKey());
      if (other != null && !Objects.equals(entry.getValue(), other)) {
        return false;
      }
    }
    return true;
  }
}
