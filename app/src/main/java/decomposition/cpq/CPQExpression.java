package decomposition.cpq;

import decomposition.core.model.Component;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.BitSet;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a CPQ AST together with its owning {@link Component}. The CPQ object is the
 * authoritative representation; {@link #cpqRule()} is only for display.
 *
 * <p>Design invariants:
 *
 * <ul>
 *   <li>CPQs capture the traversal direction via source → target, which may differ from the CQ
 *       edge.
 *   <li>Reverse edges are represented via inverse labels (r⁻) when required for intersections or
 *       joins.
 *   <li>Each instance records a derivation description for explainability/debugging.
 * </ul>
 */
public record CPQExpression(CPQ cpq, Component component, String source, String target, String derivation) {

  public CPQExpression {
    Objects.requireNonNull(cpq, "cpq");
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(derivation, "derivation");
  }

  public BitSet edges() {
    return component.edgeBits();
  }

  /**
   * Returns an immutable mapping from original CQ variables to the graph nodes used in this
   * component.
   */
  public Map<String, String> varToNodeMap() {
    return component.varMap();
  }

  /** Looks up the graph node assigned to a CQ variable, if any. */
  public String getNodeForVar(String varName) {
    return component.varMap().get(varName);
  }

  /** Looks up the original CQ variable assigned to a graph node, if any. */
  public String getVarForNode(String node) {
    if (node == null) {
      return null;
    }
    for (Map.Entry<String, String> entry : component.varMap().entrySet()) {
      if (Objects.equals(node, entry.getValue())) {
        return entry.getKey();
      }
    }
    return null;
  }

  /**
   * Returns the CPQ rule as a string for display/logging purposes only. This should not be used for
   * equality or identity comparisons.
   */
  public String cpqRule() {
    return cpq.toString();
  }
}
