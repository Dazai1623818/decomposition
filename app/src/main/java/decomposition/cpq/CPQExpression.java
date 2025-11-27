package decomposition.cpq;

import decomposition.cpq.model.CacheStats.ComponentKey;

import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a recognized component together with its CPQ AST. The CPQ object
 * is the authoritative
 * representation; toString() is only for display.
 *
 * <p>
 * Design invariants:
 *
 * <ul>
 * <li>CPQs capture the traversal direction via source → target, which may
 * differ from the CQ
 * edge.
 * <li>Reverse edges are represented via inverse labels (r⁻) when required for
 * intersections or
 * joins.
 * <li>Each instance records a derivation description for
 * explainability/debugging.
 * <li>Structural comparisons use only edge coverage + oriented endpoints; CPQ
 * syntax is
 * informational.
 * </ul>
 */
public final class CPQExpression {
  private final CPQ cpq;
  private final BitSet edges;
  private final String source;
  private final String target;
  private final String derivation;
  private final Map<String, String> varToNodeMap;
  private final Map<String, String> nodeToVarMap;

  public CPQExpression(
      CPQ cpq,
      BitSet edges,
      String source,
      String target,
      String derivation,
      Map<String, String> varToNodeMap) {
    this.cpq = Objects.requireNonNull(cpq, "cpq");
    this.edges = (BitSet) Objects.requireNonNull(edges, "edges").clone();
    this.source = Objects.requireNonNull(source, "source");
    this.target = Objects.requireNonNull(target, "target");
    this.derivation = Objects.requireNonNull(derivation, "derivation");
    this.varToNodeMap = Collections.unmodifiableMap(
        new LinkedHashMap<>(Objects.requireNonNull(varToNodeMap, "varToNodeMap")));
    this.nodeToVarMap = Collections.unmodifiableMap(invertMapping(this.varToNodeMap));
  }

  private static Map<String, String> invertMapping(Map<String, String> mapping) {
    Map<String, String> inverse = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      inverse.put(entry.getValue(), entry.getKey());
    }
    return inverse;
  }

  public CPQ cpq() {
    return cpq;
  }

  public BitSet edges() {
    return (BitSet) edges.clone();
  }

  public String source() {
    return source;
  }

  public String target() {
    return target;
  }

  public String derivation() {
    return derivation;
  }

  /**
   * Returns an immutable mapping from original CQ variables to the graph nodes
   * used in this
   * component.
   */
  public Map<String, String> varToNodeMap() {
    return varToNodeMap;
  }

  /** Looks up the graph node assigned to a CQ variable, if any. */
  public String getNodeForVar(String varName) {
    return varToNodeMap.get(varName);
  }

  /** Looks up the original CQ variable assigned to a graph node, if any. */
  public String getVarForNode(String node) {
    return nodeToVarMap.get(node);
  }

  /**
   * Returns the CPQ rule as a string for display/logging purposes only. This
   * should not be used for
   * equality or identity comparisons.
   */
  public String cpqRule() {
    return cpq.toString();
  }

  /** Creates a component key for memoization based on edges and endpoints. */
  public ComponentKey toKey(int totalEdges) {
    return new ComponentKey(edges, source, target);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CPQExpression that)) {
      return false;
    }
    return cpq.equals(that.cpq)
        && edges.equals(that.edges)
        && source.equals(that.source)
        && target.equals(that.target)
        && derivation.equals(that.derivation)
        && varToNodeMap.equals(that.varToNodeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cpq, edges, source, target, derivation, varToNodeMap);
  }

  @Override
  public String toString() {
    return "CPQExpression{"
        + "cpq="
        + cpq
        + ", edges="
        + edges
        + ", source='"
        + source
        + '\''
        + ", target='"
        + target
        + '\''
        + ", derivation='"
        + derivation
        + '\''
        + ", varToNodeMap="
        + varToNodeMap
        + '}';
  }
}
