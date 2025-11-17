package decomposition.cli;

import decomposition.eval.QueryEvaluationRunner;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the `evaluate` command that drives the native evaluator. */
final class EvaluateCommand {
  private static final Logger LOG = LoggerFactory.getLogger(EvaluateCommand.class);

  int execute(String[] args) throws IOException {
    EvaluateCliOptions options = parseEvaluateArgs(args);
    LOG.info(
        "Running evaluator for example {} using {} decomposition inputs",
        options.exampleName(),
        options.decompositionInputs().size());
    QueryEvaluationRunner runner = new QueryEvaluationRunner();
    runner.run(
        new QueryEvaluationRunner.EvaluateOptions(
            options.exampleName(),
            options.graphPath(),
            options.nativeLibrary(),
            options.decompositionInputs()));
    return 0;
  }

  private EvaluateCliOptions parseEvaluateArgs(String[] args) {
    if (args.length == 0 || !"evaluate".equalsIgnoreCase(args[0])) {
      throw new IllegalArgumentException("Unknown command: " + (args.length == 0 ? "" : args[0]));
    }
    String exampleName = "example1";
    Path graphPath = Path.of("graph_huge.edge");
    Path nativeLibrary = Path.of("lib", "libnauty.so");
    List<Path> decompositionInputs = new ArrayList<>();

    for (int i = 1; i < args.length; i++) {
      String rawArg = args[i];
      String option = rawArg;
      String inlineValue = null;
      if (rawArg.startsWith("--")) {
        int equalsIndex = rawArg.indexOf('=');
        if (equalsIndex > 0) {
          option = rawArg.substring(0, equalsIndex);
          inlineValue = rawArg.substring(equalsIndex + 1);
          if (inlineValue.isEmpty()) {
            inlineValue = null;
          }
        }
      }

      switch (option) {
        case "--example" ->
            exampleName =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--graph" ->
            graphPath =
                Path.of(
                    inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option));
        case "--native-lib" ->
            nativeLibrary =
                Path.of(
                    inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option));
        case "--decomposition" -> {
          String value =
              inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
          decompositionInputs.add(Path.of(value));
        }
        default -> throw new IllegalArgumentException("Unknown option: " + rawArg);
      }
    }

    return new EvaluateCliOptions(exampleName, graphPath, nativeLibrary, decompositionInputs);
  }

  private record EvaluateCliOptions(
      String exampleName, Path graphPath, Path nativeLibrary, List<Path> decompositionInputs) {
    EvaluateCliOptions {
      if (exampleName == null || exampleName.isBlank()) {
        exampleName = "example1";
      }
      if (graphPath == null) {
        throw new IllegalArgumentException("graph path must be provided");
      }
      if (nativeLibrary == null) {
        throw new IllegalArgumentException("native library path must be provided");
      }
      decompositionInputs =
          decompositionInputs == null
              ? List.of()
              : List.copyOf(new ArrayList<>(decompositionInputs));
    }
  }
}
