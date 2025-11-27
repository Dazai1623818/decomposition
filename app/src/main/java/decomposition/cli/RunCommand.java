package decomposition.cli;

import decomposition.core.DecompositionOptions.Mode;
import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.model.Component;
import decomposition.cpq.CPQExpression;
import decomposition.examples.RandomExampleConfig;
import decomposition.pipeline.Pipeline;
import decomposition.util.BitsetUtils;
import decomposition.util.VisualizationExporter;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  int execute(String[] args) throws IOException {
    CliOptions cliOptions = parseRunArgs(args);
    Pipeline pipeline = new Pipeline();

    if (cliOptions.buildIndexOnly()) {
      pipeline.buildIndex(cliOptions.compareGraphPath());
      return 0;
    }

    CQ query = loadQuery(cliOptions);

    DecompositionOptions pipelineOptions =
        new DecompositionOptions(
            cliOptions.mode(),
            cliOptions.maxPartitions(),
            cliOptions.timeBudgetMs(),
            cliOptions.enumerationLimit(),
            cliOptions.singleTuplePerPartition(),
            false);

    if (cliOptions.compareWithIndex()) {
      DecompositionResult result =
          pipeline.benchmark(
              query, cliOptions.freeVariables(), pipelineOptions, cliOptions.compareGraphPath());
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

    if (cliOptions.showVisualization()) {
      showComponents(result);
    }

    if (result.terminationReason() != null) {
      return 3;
    }
    return 0;
  }

  private void showComponents(DecompositionResult result) {
    for (CPQExpression component : result.recognizedCatalogue()) {
      GraphPanel.show(component.cpq());
    }
    if (result.hasFinalExpression()) {
      GraphPanel.show(result.finalExpression().cpq());
    }
  }

  private CliOptions parseRunArgs(String[] args) {
    if (args == null || args.length == 0) {
      throw new IllegalArgumentException("Missing command. Use: run [options]");
    }
    String command = args[0];
    if ("run".equalsIgnoreCase(command)) {
      return parseDecomposeArgs(asDecomposeArgs(Arrays.copyOfRange(args, 1, args.length)));
    }
    if ("decompose".equalsIgnoreCase(command)) {
      return parseDecomposeArgs(args);
    }
    if ("example".equalsIgnoreCase(command)) {
      if (args.length < 2) {
        throw new IllegalArgumentException(
            "Missing example name. Usage: example <exampleName> [options]");
      }
      String[] remapped =
          CliParsers.remapExampleArgs(args[1], Arrays.copyOfRange(args, 2, args.length));
      return parseDecomposeArgs(remapped);
    }
    if (CliParsers.isExampleAlias(command)) {
      String[] remapped =
          CliParsers.remapExampleArgs(command, Arrays.copyOfRange(args, 1, args.length));
      return parseDecomposeArgs(remapped);
    }
    throw new IllegalArgumentException("Unknown command: " + command);
  }

  private CliOptions parseDecomposeArgs(String[] args) {
    if (args.length == 0 || !"decompose".equalsIgnoreCase(args[0])) {
      throw new IllegalArgumentException("Unknown command: " + (args.length == 0 ? "" : args[0]));
    }

    String queryText = null;
    String cpqText = null;
    String queryFile = null;
    String exampleName = null;
    String freeVarsRaw = null;
    String modeRaw = null;
    String maxPartitionsRaw = null;
    String timeBudgetRaw = null;
    String outputPath = null;
    String limitRaw = null;
    boolean show = false;
    boolean singleTuplePerPartition = false;
    String randomFreeRaw = null;
    String randomEdgesRaw = null;
    String randomLabelsRaw = null;
    String randomSeedRaw = null;
    String compareGraphRaw = null;
    List<String> compareDecompositionRaw = new ArrayList<>();
    boolean compareIndex = false;
    boolean buildIndexOnly = false;

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
        case "--cq" ->
            queryText = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--cpq" ->
            cpqText = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--file" ->
            queryFile = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--example" ->
            exampleName =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--free-vars" ->
            freeVarsRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--mode" ->
            modeRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--max-partitions" ->
            maxPartitionsRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--time-budget-ms" ->
            timeBudgetRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--limit" ->
            limitRaw = inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--single-tuple" -> singleTuplePerPartition = true;
        case "--out" ->
            outputPath =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--show" -> show = true;
        case "--compare-index" -> compareIndex = true;
        case "--build-index-only" -> {
          buildIndexOnly = true;
          compareIndex = true;
        }
        case "--compare-graph" ->
            compareGraphRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--compare-decomposition" -> {
          String value =
              inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
          compareDecompositionRaw.add(value);
        }
        case "--random-free-vars" ->
            randomFreeRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--random-edges" ->
            randomEdgesRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--random-labels" ->
            randomLabelsRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        case "--random-seed" ->
            randomSeedRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
        default -> throw new IllegalArgumentException("Unknown option: " + rawArg);
      }
    }

    int sources = 0;
    if (queryText != null) sources++;
    if (cpqText != null) sources++;
    if (queryFile != null) sources++;
    if (exampleName != null) sources++;
    if (sources == 0 && !buildIndexOnly) {
      throw new IllegalArgumentException(
          "Provide exactly one of --cq, --cpq, --file, or --example");
    }
    if (sources > 1) {
      throw new IllegalArgumentException(
          "Provide at most one of --cq, --cpq, --file, or --example");
    }

    Set<String> freeVars = CliParsers.parseFreeVariables(freeVarsRaw);

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
        CliParsers.parseInt(
            limitRaw, DecompositionOptions.defaults().enumerationLimit(), "--limit");
    boolean randomOptionsProvided =
        randomFreeRaw != null
            || randomEdgesRaw != null
            || randomLabelsRaw != null
            || randomSeedRaw != null;
    if (randomOptionsProvided && !CliParsers.isRandomExampleName(exampleName)) {
      throw new IllegalArgumentException("--random-* options require --example random");
    }

    RandomExampleConfig randomExampleConfig = null;
    if (CliParsers.isRandomExampleName(exampleName)) {
      int freeVariables =
          CliParsers.parseInt(
              randomFreeRaw, RandomExampleConfig.DEFAULT_FREE_VARIABLES, "--random-free-vars");
      int edges =
          CliParsers.parseInt(
              randomEdgesRaw, RandomExampleConfig.DEFAULT_EDGE_COUNT, "--random-edges");
      int labels =
          CliParsers.parseInt(
              randomLabelsRaw, RandomExampleConfig.DEFAULT_PREDICATE_LABELS, "--random-labels");
      Long seed =
          randomSeedRaw != null ? CliParsers.parseLong(randomSeedRaw, 0L, "--random-seed") : null;
      randomExampleConfig = new RandomExampleConfig(freeVariables, edges, labels, seed);
    }

    List<Path> compareDecompositions = new ArrayList<>();
    for (String raw : compareDecompositionRaw) {
      compareDecompositions.add(Path.of(raw));
    }
    Path compareGraphPath = compareGraphRaw != null ? Path.of(compareGraphRaw) : null;
    return new CliOptions(
        queryText,
        cpqText,
        queryFile,
        exampleName,
        freeVars,
        mode,
        maxPartitions,
        timeBudget,
        limit,
        singleTuplePerPartition,
        show,
        outputPath,
        randomExampleConfig,
        compareIndex,
        compareGraphPath,
        compareDecompositions,
        buildIndexOnly);
  }

  private CQ loadQuery(CliOptions options) throws IOException {
    if (options.hasExample()) {
      return CliParsers.loadExampleByName(options.exampleName(), options.randomExampleConfig());
    }
    if (options.hasQueryText()) {
      return CQ.parse(options.queryText());
    }
    if (options.hasCpqExpression()) {
      return CliParsers.loadQueryFromCpq(options.cpqExpression());
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
          if (options.enumerationLimit() > 0 && tuples.size() >= options.enumerationLimit()) {
            LOG.info("    ... limit reached ({})", options.enumerationLimit());
          }
        }
      }
    }
  }

  private void exportForVisualization(DecompositionResult result) {
    try {
      var cwd = Paths.get("").toAbsolutePath().normalize();
      Path projectRoot = findProjectRoot(cwd);
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

  private String[] asDecomposeArgs(String[] remaining) {
    String[] remapped = new String[remaining.length + 1];
    remapped[0] = "decompose";
    System.arraycopy(remaining, 0, remapped, 1, remaining.length);
    return remapped;
  }

  private Path findProjectRoot(Path cwd) {
    Path current = cwd;
    while (current != null) {
      Path settingsHere = current.resolve("settings.gradle");
      if (Files.exists(settingsHere)) {
        return current;
      }
      Path nestedAppSettings = current.resolve("app").resolve("settings.gradle");
      if (Files.exists(nestedAppSettings)) {
        return current.resolve("app");
      }
      current = current.getParent();
    }
    return cwd;
  }
}
