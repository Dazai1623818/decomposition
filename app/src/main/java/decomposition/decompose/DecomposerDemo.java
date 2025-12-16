package decomposition.decompose;

import decomposition.examples.Example;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;

/**
 * Small demo entrypoint that runs both decomposition modes on a built-in example CQ and prints the
 * resulting CPQs.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>{@code DecomposerDemo} — uses example1
 *   <li>{@code DecomposerDemo 3} — uses example3
 * </ul>
 */
public final class DecomposerDemo {

  private DecomposerDemo() {}

  public static void main(String[] args) {
    int exampleId = args != null && args.length > 0 ? parseExampleId(args[0]) : 1;
    CQ cq = pickExample(exampleId);

    System.out.println("Running decomposition on example" + exampleId);

    List<List<CPQ>> single =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.SINGLE_EDGE);
    System.out.println("Single-edge decompositions (" + single.size() + "):");
    printDecompositions(single);

    List<List<CPQ>> exhaustive =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    System.out.println("Exhaustive decompositions (" + exhaustive.size() + "):");
    printDecompositions(exhaustive);
  }

  private static CQ pickExample(int id) {
    return switch (id) {
      case 2 -> Example.example2();
      case 3 -> Example.example3();
      case 4 -> Example.example4();
      case 5 -> Example.example5();
      case 6 -> Example.example6();
      case 7 -> Example.example7();
      case 8 -> Example.example8();
      default -> Example.example1();
    };
  }

  private static int parseExampleId(String raw) {
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException ex) {
      return 1;
    }
  }

  private static void printDecompositions(List<List<CPQ>> decompositions) {
    for (int i = 0; i < decompositions.size(); i++) {
      List<CPQ> cpqs = decompositions.get(i);
      System.out.println("  Decomposition " + (i + 1) + " (" + cpqs.size() + " CPQs):");
      cpqs.forEach(cpq -> System.out.println("    - " + cpq));
    }
  }
}
