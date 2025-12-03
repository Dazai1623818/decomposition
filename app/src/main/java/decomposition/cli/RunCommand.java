package decomposition.cli;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionOptions.Mode;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.model.Component;
import decomposition.cpq.CPQExpression;
import decomposition.pipeline.Pipeline;
import decomposition.pipeline.PlanMode;
import decomposition.util.BitsetUtils;
import decomposition.util.VisualizationExporter;
import dev.roanh.gmark.lang.cq.CQ;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the primary `run`/`decompose` command. */
final class RunCommand {
  private static final Logger LOG = LoggerFactory.getLogger(RunCommand.class);
  private static final int DEFAULT_INDEX_K = 3;

  int execute(String[] args) throws IOException {
    CliOptions cliOptions = parseDecomposeArgs(args);
    Pipeline pipeline = new Pipeline();

    if (cliOptions.buildIndexOnly()) {
      pipeline.buildIndex(cliOptions.compareGraphPath(), cliOptions.indexK());
      return 0;
    }

    CQ query = loadQuery(cliOptions);

    Mode effectiveMode = cliOptions.evaluate() ? Mode.ENUMERATE : cliOptions.mode();
    int effectiveTupleLimit = cliOptions.tupleLimit() > 0 ? cliOptions.tupleLimit() : 1;
    int diameterCap = cliOptions.evaluate() ? cliOptions.indexK() : cliOptions.diameterCap();
    DecompositionOptions pipelineOptions =
        new DecompositionOptions(
            effectiveMode,
            cliOptions.maxPartitions(),
            cliOptions.timeBudgetMs(),
            effectiveTupleLimit,
            false,
            cliOptions.planMode(),
            diameterCap);

    if (cliOptions.evaluate()) {
      DecompositionResult result =
          pipeline.benchmark(
              query,
              cliOptions.freeVariables(),
              pipelineOptions,
              cliOptions.compareGraphPath(),
              cliOptions.indexK());
      // In evaluate mode, rely on pipeline/evaluator timing logs; skip verbose summary.
      return 0;
    }

    DecompositionResult result =
        pipeline.decompose(query, cliOptions.freeVariables(), pipelineOptions);

    logSummary(result, cliOptions);
    exportForVisualization(result);

    if (result.terminationReason() != null) {
      return 3;
    }
    return 0;
  }

  private CliOptions parseDecomposeArgs(String[] args) {
    String[] effectiveArgs = stripCommand(args);
    CliOptions.Builder builder = CliOptions.builder();
    Map<String, OptionSpec> specs = optionSpecs();

    for (int i = 0; i < effectiveArgs.length; i++) {
      ParsedArg parsed = ParsedArg.parse(effectiveArgs[i]);
      OptionSpec spec = specs.get(parsed.option());
      if (spec == null) {
        throw new IllegalArgumentException("Unknown option: " + effectiveArgs[i]);
      }

      String value = parsed.value();
      if (spec.requiresValue()) {
        if (value == null || value.isBlank()) {
          if (i + 1 >= effectiveArgs.length) {
            throw new IllegalArgumentException("Missing value for " + parsed.option());
          }
          value = effectiveArgs[++i];
        }
      }
      spec.apply(builder, value);
    }

    return builder.build();
  }

  private Map<String, OptionSpec> optionSpecs() {
    Map<String, OptionSpec> specs = new LinkedHashMap<>();
    specs.put("--file", OptionSpec.withValue((b, raw) -> b.queryFile(raw)));
    specs.put("--example", OptionSpec.withValue((b, raw) -> b.exampleName(raw)));
    specs.put("--mode", OptionSpec.withValue((b, raw) -> b.mode(parseMode(raw))));
    specs.put(
        "--max-partitions",
        OptionSpec.withValue(
            (b, raw) ->
                b.maxPartitions(
                    CliParsers.parseInt(
                        raw,
                        DecompositionOptions.defaults().maxPartitions(),
                        "--max-partitions"))));
    specs.put(
        "--time-budget-ms",
        OptionSpec.withValue(
            (b, raw) ->
                b.timeBudgetMs(
                    CliParsers.parseLong(
                        raw, DecompositionOptions.defaults().timeBudgetMs(), "--time-budget-ms"))));
    specs.put("--plan", OptionSpec.withValue((b, raw) -> b.planMode(parsePlanMode(raw))));
    specs.put("--evaluate", OptionSpec.flag(b -> b.evaluate(true)));
    specs.put("--build-index-only", OptionSpec.flag(b -> b.buildIndexOnly(true)));
    specs.put(
        "--compare-graph", OptionSpec.withValue((b, raw) -> b.compareGraphPath(Path.of(raw))));
    specs.put(
        "--k",
        OptionSpec.withValue(
            (b, raw) -> b.indexK(CliParsers.parseInt(raw, DEFAULT_INDEX_K, "--index-k"))));
    return specs;
  }

  private String[] stripCommand(String[] args) {
    if (args == null || args.length == 0) {
      return new String[0];
    }
    String first = args[0];
    if ("run".equalsIgnoreCase(first) || "decompose".equalsIgnoreCase(first)) {
      return Arrays.copyOfRange(args, 1, args.length);
    }
    return args;
  }

  private Mode parseMode(String raw) {
    if (raw == null || raw.isBlank()) {
      return Mode.VALIDATE;
    }
    String normalized = raw.toLowerCase(Locale.ROOT);
    return switch (normalized) {
      case "validate" -> Mode.VALIDATE;
      case "enumerate" -> Mode.ENUMERATE;
      case "both", "decompose" -> {
        LOG.warn("--mode {} is deprecated; using --mode enumerate.", raw);
        yield Mode.ENUMERATE;
      }
      case "partitions" -> {
        LOG.warn("--mode {} is deprecated; using --mode validate.", raw);
        yield Mode.VALIDATE;
      }
      default -> throw new IllegalArgumentException("Invalid mode: " + raw);
    };
  }

  private PlanMode parsePlanMode(String raw) {
    if (raw == null || raw.isBlank()) {
      return PlanMode.ALL;
    }
    return switch (raw.toLowerCase(Locale.ROOT)) {
      case "single", "single-edge", "edges" -> PlanMode.SINGLE_EDGE;
      case "first", "first-valid" -> PlanMode.FIRST;
      case "random", "rand" -> PlanMode.RANDOM;
      case "all", "default" -> PlanMode.ALL;
      default -> throw new IllegalArgumentException("Invalid plan: " + raw);
    };
  }

  private CQ loadQuery(CliOptions options) throws IOException {
    if (options.hasExample()) {
      return CliParsers.loadExampleByName(options.exampleName());
    }

    if (options.hasQueryFile()) {
      return CliParsers.loadQueryFromFile(options.queryFile());
    }
    throw new IllegalStateException("Missing query input.");
  }

  private void logSummary(DecompositionResult result, CliOptions options) {
    int totalPartitions = result.filteredPartitionList().size();
    int validPartitions = result.cpqPartitions().size();
    LOG.info("Decomposition took {} ms", result.elapsedMillis());
    LOG.info("Partitions considered: {}", totalPartitions);
    LOG.info("Valid partitions: {}", validPartitions);
    if (validPartitions == 0) {
      LOG.info("No valid partitions identified.");
      return;
    }

    int totalEdges = result.edges().size();
    for (PartitionEvaluation evaluation : result.partitionEvaluations()) {
      LOG.info("Partition #{}:", evaluation.partitionIndex());
      List<Component> components = evaluation.partition().components();
      List<Integer> expressionCounts = evaluation.componentExpressionCounts();
      for (int i = 0; i < components.size(); i++) {
        var component = components.get(i);
        String signature = BitsetUtils.signature(component.edgeBits(), totalEdges);
        int expressionsForComponent = expressionCounts.get(i);
        LOG.info("  Component #{} [{}] expressions={}", i + 1, signature, expressionsForComponent);
      }
      if (options.mode() == Mode.ENUMERATE) {
        List<List<CPQExpression>> tuples = evaluation.decompositionTuples();
        if (tuples.isEmpty()) {
          LOG.info("  Tuples: none");
        } else {
          LOG.info("  Tuples (showing {}):", tuples.size());
          for (int i = 0; i < tuples.size(); i++) {
            List<CPQExpression> tuple = tuples.get(i);
            List<String> fragments = new ArrayList<>(tuple.size());
            for (CPQExpression component : tuple) {
              fragments.add(
                  component.cpq().toString()
                      + " ("
                      + component.source()
                      + "→"
                      + component.target()
                      + ")");
            }
            LOG.info("    #{}: {}", i + 1, String.join(" | ", fragments));
          }
          if (options.tupleLimit() > 0 && tuples.size() >= options.tupleLimit()) {
            LOG.info("    ... limit reached ({})", options.tupleLimit());
          }
        }
      }
    }
  }

  private void exportForVisualization(DecompositionResult result) {
    try {
      Path projectRoot = CliParsers.findProjectRoot();
      Path baseDir = projectRoot.resolve("temp");
      VisualizationExporter.export(
          baseDir,
          result.edges(),
          result.freeVariables(),
          result.allPartitions(),
          result.filteredPartitionList(),
          result.cpqPartitions());
    } catch (IOException ex) {
      LOG.warn("Failed to export visualization artifacts: {}", ex.getMessage());
    }
  }

  private record ParsedArg(String option, String value) {
    static ParsedArg parse(String raw) {
      if (raw == null || raw.isBlank()) {
        throw new IllegalArgumentException("Unknown option: " + raw);
      }
      if (raw.startsWith("--")) {
        int equalsIndex = raw.indexOf('=');
        if (equalsIndex > 0) {
          String option = raw.substring(0, equalsIndex);
          String value = raw.substring(equalsIndex + 1);
          return new ParsedArg(option, value.isEmpty() ? null : value);
        }
      }
      return new ParsedArg(raw, null);
    }
  }

  private record OptionSpec(boolean requiresValue, BiConsumer<CliOptions.Builder, String> apply) {
    static OptionSpec withValue(BiConsumer<CliOptions.Builder, String> consumer) {
      return new OptionSpec(true, consumer);
    }

    static OptionSpec flag(Consumer<CliOptions.Builder> consumer) {
      return new OptionSpec(false, (builder, ignored) -> consumer.accept(builder));
    }

    void apply(CliOptions.Builder builder, String value) {
      apply.accept(builder, value);
    }
  }
}
