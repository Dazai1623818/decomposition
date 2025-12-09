package decomposition.cpq;

import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Anchors loop-shaped {@link CPQExpression} instances to enforce equality constraints. */
final class ReverseLoopGenerator {
  private final ComponentEdgeMatcher validator;

  ReverseLoopGenerator(ComponentEdgeMatcher validator) {
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  List<CPQExpression> generate(CPQExpression rule, Map<String, String> originalVarMap) {
    CPQExpression candidate = rule;
    if (rule.source().equals(rule.target())) {
      try {
        if (!rule.cpq().toQueryGraph().isLoop()) {
          CPQ anchoredCpq = CPQ.intersect(rule.cpq(), CPQ.IDENTITY);
          candidate =
              new CPQExpression(
                  anchoredCpq,
                  rule.edges(),
                  rule.source(),
                  rule.target(),
                  rule.derivation() + " + anchored with id",
                  originalVarMap);
        }
      } catch (RuntimeException ignored) {
        // Keep original if graph extraction fails
      }
    }

    if (validator.isValid(candidate)) {
      return List.of(candidate);
    }
    return List.of();
  }
}
