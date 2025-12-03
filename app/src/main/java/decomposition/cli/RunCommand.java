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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the primary `run`/`decompose` command. */
final class RunCommand {
  private static final Logger LOG = LoggerFactory.getLogger(RunCommand.class);
  private static final int DEFAULT_INDEX_K = 3;

  int execute(String[] args) throws IOException {
    CliOptions cliOptions = parseRunArgs(args);
    Pipeline pipeline = new Pipeline();

    if (cliOptions.buildIndexOnly()) {
      pipeline.buildIndex(cliOptions.compareGraphPath(), cliOptions.indexK());
      return 0;
    }

    CQ query = loadQuery(cliOptions);

    DecompositionOptions pipelineOptions =
        new DecompositionOptions(
            cliOptions.mode(),
            cliOptions.maxPartitions(),
            cliOptions.timeBudgetMs(),
            cliOptions.tupleLimit(),
            false,
            cliOptions.planMode());

    if (cliOptions.compareWithIndex()) {
      DecompositionResult result =
          pipeline.benchmark(
              query,
              cliOptions.freeVariables(),
              pipelineOptions,
              cliOptions.compareGraphPath(),
              cliOptions.indexK());
      logSummary(result, cliOptions);
      exportForVisualization(result);
      return 0;
    }

    DecompositionResult result =
        pipeline.decompose(query, cliOptions.freeVariables(), pipelineOptions);

    logSummary(result, cliOptions);
    exportForVisualization(result);

    JsonReportBuilder reportBuilder = new JsonReportBuilder();
    String json = reportBuilder.build(result);

    if (cliOptions.outputPath() != null) {
      writeOutput(cliOptions.outputPath(), json);
    } else {
      System.out.println(json);
    }

    if (result.terminationReason() != null) {
      return 3;
    }
    return 0;
  }

  private CliOptions parseRunArgs(String[] args) {
    if (args == null || args.length == 0) {
      throw new IllegalArgumentException("Missing command. Use: run [options]");
    }
    String command = args[0];
    if ("run".equalsIgnoreCase(command)) {
      return parseDecomposeArgs(
          CliParsers.asDecomposeArgs(Arrays.copyOfRange(args, 1, args.length)));
    }
    if ("decompose".equalsIgnoreCase(command)) {
      return parseDecomposeArgs(args);
    }

    throw new IllegalArgumentException("Unknown command: " + command);
  }

  private CliOptions parseDecomposeArgs(String[] args) {
    if (args.length == 0 || !"decompose".equalsIgnoreCase(args[0])) {
      throw new IllegalArgumentException("Unknown command: " + (args.length == 0 ? "" : args[0]));
    }

    String queryFile = null;
    String exampleName = null;

    String modeRaw = null;
    String maxPartitionsRaw = null;
    String timeBudgetRaw = null;
    String outputPath = null;
    String limitRaw = null;
    String planRaw = null;
    Integer tupleLimit = null;
    String compareGraphRaw = null;
    boolean compareIndex = false;
    boolean buildIndexOnly = false;
    String indexKRaw = null;

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
        case "--file" ->
            queryFile = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--example" ->
            exampleName =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);

        case "--mode" ->
            modeRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--max-partitions" ->
            maxPartitionsRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--time-budget-ms" ->
            timeBudgetRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--plan" ->
            planRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--limit" ->
            limitRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--single-tuple" -> tupleLimit = 1;
        case "--out" ->
            outputPath =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--compare-index" -> compareIndex = true;
        case "--build-index-only" -> {
          buildIndexOnly = true;
          compareIndex = true;
        }
        case "--compare-graph" ->
            compareGraphRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--k" ->
            indexKRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        default -> throw new IllegalArgumentException("Unknown option: " + rawArg);
      }
    }

    int sources = 0;
    if (queryFile != null) sources++;
    if (exampleName != null) sources++;
    if (sources == 0 && !buildIndexOnly) {
      throw new IllegalArgumentException("Provide exactly one of --file or --example");
    }
    if (sources > 1) {
      throw new IllegalArgumentException("Provide at most one of --file or --example");
    }

    Mode mode = Mode.VALIDATE;
    if (modeRaw != null) {
      String normalized = modeRaw.toLowerCase(Locale.ROOT);
      switch (normalized) {
        case "validate" -> mode = Mode.VALIDATE;
        case "enumerate" -> mode = Mode.ENUMERATE;
        case "both", "decompose" -> {
          mode = Mode.ENUMERATE;
          LOG.warn("--mode {} is deprecated; using --mode enumerate.", modeRaw);
        }
        case "partitions" -> {
          mode = Mode.VALIDATE;
          LOG.warn("--mode {} is deprecated; using --mode validate.", modeRaw);
        }
        default -> throw new IllegalArgumentException("Invalid mode: " + modeRaw);
      }
    }

    int maxPartitions =
        CliParsers.parseInt(
            maxPartitionsRaw, DecompositionOptions.defaults().maxPartitions(), "--max-partitions");
    long timeBudget =
        CliParsers.parseLong(
            timeBudgetRaw, DecompositionOptions.defaults().timeBudgetMs(), "--time-budget-ms");
    int limit =
        CliParsers.parseInt(limitRaw, DecompositionOptions.defaults().tupleLimit(), "--limit");
    if (tupleLimit != null) {
      limit = tupleLimit;
    }
    PlanMode planMode = parsePlanMode(planRaw);
    Path compareGraphPath = compareGraphRaw != null ? Path.of(compareGraphRaw) : null;
    int indexK = CliParsers.parseInt(indexKRaw, DEFAULT_INDEX_K, "--index-k");
    return new CliOptions(
        queryFile,
        exampleName,
        Set.of(),
        mode,
        maxPartitions,
        timeBudget,
        limit,
        planMode,
        outputPath,
        compareIndex,
        compareGraphPath,
        indexK,
        buildIndexOnly);
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

  private void writeOutput(String outputPath, String json) throws IOException {
    Path path = Path.of(outputPath);
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(path, json);
  }

  private PlanMode parsePlanMode(String raw) {
    if (raw == null || raw.isBlank()) {
      return PlanMode.ALL;
    }
    return switch (raw.toLowerCase(Locale.ROOT)) {
      case "single", "single-edge", "edges" -> PlanMode.SINGLE_EDGE;
      case "first", "first-valid" -> PlanMode.FIRST;
      case "all", "default" -> PlanMode.ALL;
      default -> throw new IllegalArgumentException("Invalid plan: " + raw);
    };
  }
}
