package decomposition.cli;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
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
      Operation operation = Operation.from(args[0]);
      String[] remaining = Arrays.copyOfRange(args, 1, args.length);
      return switch (operation) {
        case PLOT -> new PlotCommand().execute(remaining);
        case DECOMPOSE -> new RunCommand().execute(remaining);
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

  enum Operation {
    DECOMPOSE("run", "decompose"),
    PLOT("plot");

    private final Set<String> aliases;

    Operation(String... aliases) {
      this.aliases = new HashSet<>();
      for (String alias : aliases) {
        this.aliases.add(alias.toLowerCase(Locale.ROOT));
      }
    }

    static Operation from(String raw) {
      if (raw == null || raw.isBlank()) {
        throw new IllegalArgumentException("Missing command. Use: run [options]");
      }
      String normalized = raw.toLowerCase(Locale.ROOT);
      for (Operation op : Operation.values()) {
        if (op.aliases.contains(normalized)) {
          return op;
        }
      }
      throw new IllegalArgumentException("Unknown command: " + raw);
    }
  }
}
