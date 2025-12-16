package decomposition.decompose;

import decomposition.core.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphNode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Public wrapper for decomposing a CQ.
 *
 * <p>This wrapper intentionally keeps using gmark's {@link CQ} as the underlying representation for
 * now. Construct directly with the gmark {@link CQ}; the goal is to provide a single entry point
 * ({@link #decompose(Decomposer.DecompositionMethod)}) while the internal code is incrementally
 * reorganised.
 */
public final class ConjunctiveQuery {

  private final CQ gmarkCQ;
  private final UniqueGraph<VarCQ, AtomCQ> graph;
  private final List<GraphEdge<VarCQ, AtomCQ>> graphEdges;
  private final int[][] edgeVars;
  private final BitSet[] incidentEdges;
  private final BitSet freeVars;
  private final int edgeCount;
  private final int varCount;
  private final List<Edge> edgesDto;

  public ConjunctiveQuery(CQ gmarkCQ) {
    this.gmarkCQ = Objects.requireNonNull(gmarkCQ, "gmarkCQ");
    this.graph = gmarkCQ.toQueryGraph().toUniqueGraph();
    this.graphEdges = new ArrayList<>(graph.getEdges());
    this.edgeCount = graphEdges.size();
    this.varCount = graph.getNodeCount();

    this.edgeVars = new int[edgeCount][2];
    this.incidentEdges = new BitSet[varCount];
    for (int i = 0; i < varCount; i++) {
      incidentEdges[i] = new BitSet(edgeCount);
    }

    for (int edgeId = 0; edgeId < graphEdges.size(); edgeId++) {
      GraphEdge<VarCQ, AtomCQ> edge = graphEdges.get(edgeId);
      int srcId = edge.getSourceNode().getID();
      int dstId = edge.getTargetNode().getID();
      edgeVars[edgeId][0] = srcId;
      edgeVars[edgeId][1] = dstId;
      incidentEdges[srcId].set(edgeId);
      incidentEdges[dstId].set(edgeId);
    }

    this.freeVars = new BitSet(varCount);
    for (VarCQ free : gmarkCQ.getFreeVariables()) {
      GraphNode<VarCQ, AtomCQ> node = graph.getNode(free);
      if (node != null) {
        freeVars.set(node.getID());
      }
    }

    List<Edge> dto = new ArrayList<>(graphEdges.size());
    long syntheticId = 0L;
    for (GraphEdge<VarCQ, AtomCQ> graphEdge : graphEdges) {
      AtomCQ atom = graphEdge.getData();
      String source = atom.getSource().getName();
      String target = atom.getTarget().getName();
      dto.add(new Edge(source, target, atom.getLabel(), syntheticId++));
    }
    this.edgesDto = dto;
  }

  /** Returns the underlying gmark CQ (intended for internal use). */
  CQ gmarkCQ() {
    return gmarkCQ;
  }

  /** Returns the CQ edges in the intermediate {@link Edge} model. */
  public List<Edge> getEdges() {
    return edgesDto;
  }

  /** Decomposes this CQ using {@link Decomposer.DecompositionMethod#SINGLE_EDGE}. */
  public List<CPQ> decompose() {
    return decompose(Decomposer.DecompositionMethod.SINGLE_EDGE);
  }

  public List<CPQ> decompose(Decomposer.DecompositionMethod method) {
    Objects.requireNonNull(method, "method");
    return Decomposer.decompose(this, method);
  }

  // ----- Internal indexed view (built at construction) -----

  UniqueGraph<VarCQ, AtomCQ> graph() {
    return graph;
  }

  List<GraphEdge<VarCQ, AtomCQ>> graphEdges() {
    return graphEdges;
  }

  int edgeCount() {
    return edgeCount;
  }

  int varCount() {
    return varCount;
  }

  BitSet edgeVarsBitSet(int edgeId) {
    BitSet bits = new BitSet(varCount);
    bits.set(edgeVars[edgeId][0]);
    bits.set(edgeVars[edgeId][1]);
    return bits;
  }

  BitSet incidentEdges(int varId) {
    return incidentEdges[varId];
  }

  BitSet freeVarsBitSet() {
    return (BitSet) freeVars.clone();
  }

  BitSet joinVars(BitSet edgeBits, BitSet varBits) {
    // Free variables that appear inside the component are always join variables.
    BitSet join = (BitSet) varBits.clone();
    join.and(freeVars);

    for (int var = varBits.nextSetBit(0); var >= 0; var = varBits.nextSetBit(var + 1)) {
      BitSet outside = (BitSet) incidentEdges[var].clone();
      outside.andNot(edgeBits);
      if (!outside.isEmpty()) {
        join.set(var);
      }
    }
    return join;
  }
}
