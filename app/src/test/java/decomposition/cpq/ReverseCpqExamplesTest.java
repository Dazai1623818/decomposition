package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import decomposition.core.model.Edge;
import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

final class ReverseCpqExamplesTest {

  @Disabled("Reverse is removed")
  @Test
  void printsSyntaxTreesForOriginalAndReversedCpqs() {
    List<String> expressions =
        List.of(
            "r1",
            "r1⁻",
            "(r1◦r2)",
            "(r1∩r2)",
            "((r1◦r2)∩r3)",
            "((r1∩r2)◦r3)",
            "(((r1◦r2)◦r3)∩(r4⁻◦r5))",
            "(((r1∩r2)◦(r3∩r4⁻))∩id)",
            "(r1◦(r2∩(r3◦r4⁻)))",
            "((r1∩(r2◦r3))◦(r4∩r5⁻))");

    ReverseLoopGenerator generator =
        new ReverseLoopGenerator(new ComponentEdgeMatcher(List.<Edge>of()));
    Method reverseMethod = reverseMethod();

    for (int i = 0; i < expressions.size(); i++) {
      String expr = expressions.get(i);
      CPQ original = CPQ.parse(expr);
      CPQ reversed = reverse(generator, reverseMethod, original);

      System.out.println("CPQ #" + (i + 1) + " original: " + original);
      System.out.println(formatTree(original));
      System.out.println("CPQ #" + (i + 1) + " reversed: " + reversed);
      System.out.println(formatTree(reversed));
      System.out.println("=".repeat(72));

      CPQ doubleReversed = reverse(generator, reverseMethod, reversed);
      assertEquals(
          normalize(original.toString()),
          normalize(doubleReversed.toString()),
          "Reversing twice should restore the original syntax");
    }
  }

  private static Method reverseMethod() {
    try {
      Method method = ReverseLoopGenerator.class.getDeclaredMethod("reverseCpq", CPQ.class);
      method.setAccessible(true);
      return method;
    } catch (NoSuchMethodException ex) {
      throw new IllegalStateException("ReverseLoopGenerator#reverseCpq not available", ex);
    }
  }

  private static CPQ reverse(ReverseLoopGenerator generator, Method method, CPQ cpq) {
    try {
      return (CPQ) method.invoke(generator, cpq);
    } catch (IllegalAccessException | InvocationTargetException ex) {
      throw new IllegalStateException("Unable to reverse CPQ: " + cpq, ex);
    }
  }

  private static String formatTree(CPQ cpq) {
    StringBuilder out = new StringBuilder();
    appendTree(cpq.toAbstractSyntaxTree(), out, 0, "root");
    return out.toString();
  }

  private static void appendTree(QueryTree node, StringBuilder out, int depth, String label) {
    out.append("  ".repeat(depth)).append(label).append(": ").append(describe(node)).append('\n');
    for (int i = 0; i < node.getArity(); i++) {
      appendTree(node.getOperand(i), out, depth + 1, childLabel(node.getOperation(), i));
    }
  }

  private static String childLabel(OperationType op, int index) {
    return switch (op) {
      case CONCATENATION -> "concat-" + (index + 1);
      case INTERSECTION -> "inter-" + (index + 1);
      default -> "child-" + (index + 1);
    };
  }

  private static String describe(QueryTree node) {
    OperationType op = node.getOperation();
    return switch (op) {
      case EDGE -> "EDGE[" + node.getEdgeAtom().getLabel().getAlias() + "]";
      case IDENTITY -> "IDENTITY";
      case CONCATENATION -> "CONCAT";
      case INTERSECTION -> "INTERSECT";
      default -> op.name();
    };
  }

  private static String normalize(String expression) {
    return expression.replaceAll("\\s+", "");
  }
}
