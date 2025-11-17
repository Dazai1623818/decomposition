package decomposition.cli;

import com.google.common.base.Splitter;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the `plot` command used to visualize CPQ expressions. */
final class PlotCommand {
  private static final Logger LOG = LoggerFactory.getLogger(PlotCommand.class);

  int execute(String[] args) throws IOException {
    if (args.length == 0 || !"plot".equalsIgnoreCase(args[0])) {
      throw new IllegalArgumentException("Unknown command: " + (args.length == 0 ? "" : args[0]));
    }
    if (args.length == 1) {
      return runInteractive();
    }
    return runFromArgs(args);
  }

  private int runFromArgs(String[] args) {
    String rawInput = String.join(" ", stripCommand(args)).trim();
    if (rawInput.isEmpty()) {
      throw new IllegalArgumentException("Provide one or more CPQ expressions to plot.");
    }

    List<String> expressions = new ArrayList<>();
    Iterable<String> tokens =
        Splitter.onPattern("[\\r\\n;]+").trimResults().omitEmptyStrings().split(rawInput);
    for (String token : tokens) {
      expressions.add(token);
    }

    if (expressions.isEmpty()) {
      throw new IllegalArgumentException("No valid CPQ expressions found in input.");
    }

    int failures = 0;
    for (int i = 0; i < expressions.size(); i++) {
      if (!plotExpression(expressions.get(i), i + 1)) {
        failures++;
      }
    }
    return failures == 0 ? 0 : 2;
  }

  private int runInteractive() {
    LOG.info("Plot interactive mode");
    LOG.info("Enter CPQ expressions to plot. Submit an empty line to skip, Ctrl-D to exit.");

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
      return 0;
    } catch (IOException ex) {
      LOG.error("Error reading input", ex);
      return 1;
    }
  }

  private boolean plotExpression(String originalExpression, int index) {
    String normalized = originalExpression.replaceAll("\\s+", "");
    try {
      CPQ cpq = CPQ.parse(normalized);
      LOG.info("Parsed CPQ #{}: {}", index, cpq);
      LOG.info("  Diameter: {}", cpq.getDiameter());
      GraphPanel.show(cpq);
      return true;
    } catch (IllegalArgumentException ex) {
      LOG.error(
          "Unable to parse CPQ #{} ({}): {}",
          index,
          originalExpression,
          ex.getMessage() != null ? ex.getMessage() : ex.toString());
      return false;
    }
  }

  private String[] stripCommand(String[] args) {
    if (args.length <= 1) {
      return new String[0];
    }
    String[] remaining = new String[args.length - 1];
    System.arraycopy(args, 1, remaining, 0, remaining.length);
    return remaining;
  }
}
