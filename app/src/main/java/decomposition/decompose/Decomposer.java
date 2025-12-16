package decomposition.decompose;

import decomposition.core.model.Edge;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphNode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Performs CQ -> CPQ decompositions for gMark queries. */
public final class Decomposer {

  /** High-level decomposition strategy selection. */
  public enum DecompositionMethod {
    SINGLE_EDGE,
    EXHAUSTIVE_ENUMERATION
  }

  private interface DecompositionStrategy {
    List<List<CPQ>> decompose(CQ cq);
  }

  private Decomposer() {}

  public static List<List<CPQ>> decompose(CQ cq, DecompositionMethod method) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return strategyFor(method).decompose(cq);
  }

  static List<List<CPQ>> decompose(ConjunctiveQuery query, DecompositionMethod method) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return strategyFor(method).decompose(query.gmarkCQ());
  }

  private static DecompositionStrategy strategyFor(DecompositionMethod method) {
    return switch (method) {
      case SINGLE_EDGE -> SingleEdge.INSTANCE;
      case EXHAUSTIVE_ENUMERATION -> ExhaustiveEnumeration.INSTANCE;
    };
  }

  /** Single-edge decomposition: one CPQ per CQ edge label. */
  private static final class SingleEdge implements DecompositionStrategy {
    private static final SingleEdge INSTANCE = new SingleEdge();

    private SingleEdge() {}

    @Override
    public List<List<CPQ>> decompose(CQ cq) {
      UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
      List<CPQ> cpqs = new ArrayList<>(graph.getEdges().size());

      for (var edge : graph.getEdges()) {
        CPQ cpq = CPQ.label(edge.getData().getLabel());
        VarCQ source = edge.getSourceNode().getData();
        VarCQ target = edge.getTargetNode().getData();
        if (source.equals(target)) {
          cpq = CPQ.intersect(List.of(cpq, CPQ.id()));
        }
        cpqs.add(cpq);
      }

      return List.of(List.copyOf(cpqs));
    }
  }

  /** Exhaustive enumeration decomposition. */
  private static final class ExhaustiveEnumeration implements DecompositionStrategy {
    private static final ExhaustiveEnumeration INSTANCE = new ExhaustiveEnumeration();

    private ExhaustiveEnumeration() {}

    @Override
    public List<List<CPQ>> decompose(CQ cq) {
      ConjunctiveQuery query = new ConjunctiveQuery(cq);
      List<ExhaustiveEnumerator.Partition> partitions =
          ExhaustiveEnumerator.enumeratePartitions(cq);
      if (partitions.isEmpty()) {
        return List.of();
      }

      List<Edge> edges = query.getEdges();
      ComponentExpressionBuilder builder = new ComponentExpressionBuilder(edges);
      Map<Integer, String> varIdToName = buildVarIdToName(query.graph());
      Map<String, String> originalVarMap = buildOriginalVarMap(query.graph());
      BitSet fullMask = new BitSet(edges.size());
      fullMask.set(0, edges.size());
      BitSet fullVarBits = new BitSet(query.varCount());
      for (int edgeId = 0; edgeId < edges.size(); edgeId++) {
        fullVarBits.or(query.edgeVarsBitSet(edgeId));
      }

      List<List<CPQ>> decompositions = new ArrayList<>();
      addWholeQueryCandidates(
          builder,
          originalVarMap,
          varIdToName,
          fullMask,
          query.joinVars(fullMask, fullVarBits),
          decompositions);

      for (ExhaustiveEnumerator.Partition partition : partitions) {
        List<List<CPQExpression>> perComponent = new ArrayList<>();
        boolean valid = true;
        for (ExhaustiveEnumerator.Component component : partition.components()) {
          Set<String> joinNodes = toJoinNodes(component.joinVars(), varIdToName);
          List<CPQExpression> rules =
              builder.build(component.edgeBits(), joinNodes, originalVarMap, 0, false);
          List<CPQExpression> deduped = ComponentExpressionBuilder.dedupeExpressions(rules);
          if (deduped.isEmpty()) {
            valid = false;
            break;
          }
          perComponent.add(deduped);
        }
        if (valid) {
          expandComponentChoices(
              perComponent,
              0,
              new ArrayList<>(),
              new BitSet(edges.size()),
              fullMask,
              decompositions);
        }
      }

      return List.copyOf(decompositions);
    }

    private void addWholeQueryCandidates(
        ComponentExpressionBuilder builder,
        Map<String, String> originalVarMap,
        Map<Integer, String> varIdToName,
        BitSet fullMask,
        BitSet fullJoinVars,
        List<List<CPQ>> decompositions) {
      Set<String> joinNodes = toJoinNodes(fullJoinVars, varIdToName);
      List<CPQExpression> rules = builder.build(fullMask, joinNodes, originalVarMap, 0, false);
      List<CPQExpression> deduped = ComponentExpressionBuilder.dedupeExpressions(rules);
      for (CPQExpression expression : deduped) {
        decompositions.add(List.of(expression.cpq()));
      }
    }

    private void expandComponentChoices(
        List<List<CPQExpression>> components,
        int index,
        List<CPQ> current,
        BitSet covered,
        BitSet fullMask,
        List<List<CPQ>> out) {

      if (index == components.size()) {
        if (covered.equals(fullMask)) {
          out.add(List.copyOf(current));
        }
        return;
      }

      for (CPQExpression expression : components.get(index)) {
        BitSet nextCovered = (BitSet) covered.clone();
        nextCovered.or(expression.edges());
        current.add(expression.cpq());
        expandComponentChoices(components, index + 1, current, nextCovered, fullMask, out);
        current.remove(current.size() - 1);
      }
    }

    private Map<Integer, String> buildVarIdToName(UniqueGraph<VarCQ, AtomCQ> graph) {
      Map<Integer, String> mapping = new LinkedHashMap<>();
      for (GraphNode<VarCQ, AtomCQ> node : graph.getNodes()) {
        mapping.put(node.getID(), node.getData().getName());
      }
      return mapping;
    }

    private Map<String, String> buildOriginalVarMap(UniqueGraph<VarCQ, AtomCQ> graph) {
      Map<String, String> map = new LinkedHashMap<>();
      for (GraphNode<VarCQ, AtomCQ> node : graph.getNodes()) {
        String name = node.getData().getName();
        map.put(name, name);
      }
      return map;
    }

    private Set<String> toJoinNodes(BitSet joinVars, Map<Integer, String> varIdToName) {
      Set<String> names = new LinkedHashSet<>();
      for (int var = joinVars.nextSetBit(0); var >= 0; var = joinVars.nextSetBit(var + 1)) {
        String name = varIdToName.get(var);
        if (name != null) {
          names.add(name);
        }
      }
      return names;
    }
  }
}
