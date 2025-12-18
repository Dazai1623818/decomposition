package decomposition.cpq;

import decomposition.core.model.Component;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.Objects;

/**
 * Represents a CPQ AST together with its owning {@link Component}. The CPQ object is the
 * authoritative representation.
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
public record CPQExpression(
    CPQ cpq, Component component, String source, String target, String derivation) {

  public CPQExpression {
    Objects.requireNonNull(cpq, "cpq");
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(derivation, "derivation");
  }
}
