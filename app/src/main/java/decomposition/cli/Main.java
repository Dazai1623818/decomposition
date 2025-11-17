package decomposition.cli;

import java.io.IOException;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CLI entry point for the CQ to CPQ decomposition pipeline and utilities. */
public final class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    int exitCode = new Main().run(args);
    System.exit(exitCode);
  }

  int run(String[] args) {
    try {
      if (args == null || args.length == 0) {
        throw new IllegalArgumentException("Missing command. Use: run [options]");
      }
      String command = args[0].toLowerCase(Locale.ROOT);
      return switch (command) {
        case "profile" -> new ProfileCommand().execute(args);
        case "plot" -> new PlotCommand().execute(args);
        case "evaluate" -> new EvaluateCommand().execute(args);
        default -> new RunCommand().execute(args);
      };
    } catch (IllegalArgumentException ex) {
      LOG.error("CLI error: {}", ex.getMessage());
      LOG.debug("Argument parsing failed", ex);
      return 2;
    } catch (IOException ex) {
      LOG.error("I/O error during execution", ex);
      return 2;
    } catch (RuntimeException ex) {
      LOG.error("Unexpected failure while running command", ex);
      return 4;
    }
  }
}
