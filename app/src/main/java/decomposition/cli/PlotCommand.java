package decomposition.cli;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the `plot` command used to visualize CPQ expressions. */
public final class PlotCommand {
  private static final Logger LOG = LoggerFactory.getLogger(PlotCommand.class);

  public static void main(String[] args) {
    int exit = new PlotCommand().execute(args);
    if (exit != 0) {
      System.exit(exit);
    }
  }

  int execute(String[] args) {
    if (args != null && args.length > 0) {
      LOG.info("Ignoring positional plot arguments; running interactive mode instead: {}", Arrays.toString(args));
    }
    return runInteractive();
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
      String reason = ex.getMessage() != null ? ex.getMessage() : ex.toString();
      LOG.error("Unable to parse CPQ #{} ({}): {}", index, originalExpression, reason);
      return false;
    }
  }
}
