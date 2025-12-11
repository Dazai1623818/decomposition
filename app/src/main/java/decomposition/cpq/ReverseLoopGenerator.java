package decomposition.cpq;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Creates anchored and reversed variants for derived {@link CPQExpression} instances. */
final class ReverseLoopGenerator {
  private final ComponentEdgeMatcher validator;

  ReverseLoopGenerator(ComponentEdgeMatcher validator) {
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  List<CPQExpression> generate(CPQExpression rule, Map<String, String> originalVarMap) {
    List<CPQExpression> variants = new ArrayList<>(2);

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

    if (!candidate.source().equals(candidate.target())) {
      CPQExpression reversed = createReversedVariant(candidate, originalVarMap);
      if (reversed != null && validator.isValid(reversed)) {
        variants.add(reversed);
      }
    }

    return variants.isEmpty() ? List.of() : List.copyOf(variants);
  }

  private CPQExpression createReversedVariant(
      CPQExpression rule, Map<String, String> originalVarMap) {
    try {
      CPQ reversedCpq = reverseCpq(rule.cpq());
      return new CPQExpression(
          reversedCpq,
          rule.edges(),
          rule.target(),
          rule.source(),
          rule.derivation() + " + reversed orientation",
          originalVarMap);
    } catch (RuntimeException ex) {
      return null;
    }
  }

  private CPQ reverseCpq(CPQ cpq) {
    return reverseQueryTree(cpq.toAbstractSyntaxTree());
  }

  private CPQ reverseQueryTree(QueryTree tree) {
    OperationType operation = tree.getOperation();
    return switch (operation) {
      case EDGE -> {
        var label = tree.getEdgeAtom().getLabel();
        yield CPQ.label(label.getInverse());
      }
      case IDENTITY -> CPQ.IDENTITY;
      case CONCATENATION -> CPQ.concat(reverseOperands(tree, true));
      case INTERSECTION -> CPQ.intersect(reverseOperands(tree, false));
      default ->
          throw new IllegalArgumentException(
              "Unsupported CPQ operation for reversal: " + operation);
    };
  }

  private List<CPQ> reverseOperands(QueryTree tree, boolean reverseOrder) {
    int arity = tree.getArity();
    List<CPQ> operands = new ArrayList<>(arity);
    if (reverseOrder) {
      for (int i = arity - 1; i >= 0; i--) {
        operands.add(reverseQueryTree(tree.getOperand(i)));
      }
    } else {
      for (int i = 0; i < arity; i++) {
        operands.add(reverseQueryTree(tree.getOperand(i)));
      }
    }
    return operands;
  }
}
