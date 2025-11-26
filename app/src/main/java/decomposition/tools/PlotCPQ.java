package decomposition.tools;

import com.google.common.base.Splitter;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Small helper entry point that opens a Swing window showing the structure of a CPQ.
 *
 * <p>Usage examples:
 *
 * <pre>
 *   ./gradlew runPlot --args='r1'
 *   ./gradlew runPlot --args='(r1 ◦ (r2 ∩ r3⁻))'
 *   ./app/gradlew runPlot --args='r1; (r2 ◦ r3⁻)'    (from the repository root)
 * </pre>
 */
public final class PlotCPQ {

  private PlotCPQ() {
    // utility
  }

  public static void main(String[] args) {
    if (args.length > 0) {
      runFromArgs(args);
    } else {
      runInteractive();
    }
  }

  private static void runFromArgs(String[] args) {
    String rawInput = String.join(" ", args).trim();
    if (rawInput.isEmpty()) {
      System.err.println("Provide one or more CPQ expressions to plot.");
      System.err.println("Separate multiple expressions with ';' or newlines.");
      System.err.println(
          "Example: ./gradlew runPlot --args='r1; (r2 ◦ r3⁻)' (run inside the app directory)");
      System.exit(1);
    }

    List<String> expressions = new ArrayList<>();
    Iterable<String> tokens =
        Splitter.onPattern("[\\r\\n;]+").trimResults().omitEmptyStrings().split(rawInput);
    for (String token : tokens) {
      expressions.add(token);
    }

    if (expressions.isEmpty()) {
      System.err.println("No valid CPQ expressions found in input.");
      System.exit(1);
    }

    for (int i = 0; i < expressions.size(); i++) {
      if (!plotExpression(expressions.get(i), i + 1)) {
        System.exit(2);
      }
    }
  }

  private static void runInteractive() {
    System.out.println("RunPlot interactive mode");
    System.out.println(
        "Enter CPQ expressions to plot. Submit an empty line to skip, or Ctrl-D to exit.");

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      String line;
      int count = 1;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
          continue;
        }

        if (plotExpression(trimmed, count)) {
          count++;
        }
      }
    } catch (IOException ex) {
      System.err.println("Error reading input: " + ex.getMessage());
      System.exit(1);
    }
  }

  private static boolean plotExpression(String originalExpression, int index) {
    String normalized = originalExpression.replaceAll("\\s+", "");
    try {
      CPQ cpq = CPQ.parse(normalized);
      System.out.println("Parsed CPQ #" + index + ": " + cpq);
      System.out.println("Diameter: " + cpq.getDiameter());
      GraphPanel.show(cpq);
      return true;
    } catch (IllegalArgumentException ex) {
      System.err.println(
          "Unable to parse CPQ #" + index + " (" + originalExpression + "): " + ex.getMessage());
      return false;
    }
  }
}
