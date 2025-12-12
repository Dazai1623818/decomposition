package decomposition.cpq;

import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Creates anchored loop variants for derived {@link CPQExpression} instances. */
final class LoopVariantGenerator {
  private final ComponentEdgeMatcher validator;

  LoopVariantGenerator(ComponentEdgeMatcher validator) {
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  List<CPQExpression> generate(CPQExpression rule, Map<String, String> originalVarMap) {
    List<CPQExpression> variants = new ArrayList<>(1);

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
      variants.add(candidate);
    }

    return variants.isEmpty() ? List.of() : List.copyOf(variants);
  }
}
