package decomposition.cli;

import decomposition.DecompositionOptions;
import decomposition.DecompositionOptions.Mode;
import decomposition.DecompositionPipeline;
import decomposition.DecompositionResult;
import decomposition.Example;
import decomposition.PartitionEvaluation;
import decomposition.RandomExampleConfig;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.util.BitsetUtils;
import decomposition.util.VisualizationExporter;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** CLI entry point for the CQ to CPQ decomposition pipeline. */
public final class Main {

  public static void main(String[] args) {
    int exitCode = new Main().run(args);
    System.exit(exitCode);
  }

  int run(String[] args) {
    try {
      CliOptions cliOptions = parseArgs(args);
      DecompositionOptions pipelineOptions =
          new DecompositionOptions(
              cliOptions.mode(),
              cliOptions.maxPartitions(),
              cliOptions.maxCovers(),
              cliOptions.timeBudgetMs(),
              cliOptions.enumerationLimit());

      CQ query = loadQuery(cliOptions);

      DecompositionPipeline pipeline = new DecompositionPipeline();
      DecompositionResult result =
          pipeline.execute(query, cliOptions.freeVariables(), pipelineOptions);

      printSummary(result, cliOptions);
      exportForVisualization(result);

      JsonReportBuilder reportBuilder = new JsonReportBuilder();
      String json = reportBuilder.build(result);

      if (cliOptions.outputPath() != null) {
        writeOutput(cliOptions.outputPath(), json);
      } else {
        System.out.println(json);
      }

      if (cliOptions.showVisualization()) {
        for (KnownComponent component : result.recognizedCatalogue()) {
          GraphPanel.show(component.cpq());
        }
        if (result.hasFinalComponent()) {
          GraphPanel.show(result.finalComponent().cpq());
        }
      }

      if (result.terminationReason() != null) {
        return 3;
      }
      return 0;
    } catch (IllegalArgumentException | IOException ex) {
      System.err.println("Error: " + ex.getMessage());
      return 2;
    } catch (RuntimeException ex) {
      ex.printStackTrace(System.err);
      return 4;
    }
  }

  private void printSummary(DecompositionResult result, CliOptions options) {
    int totalPartitions = result.filteredPartitionList().size();
    int validPartitions = result.cpqPartitions().size();
    System.out.println("Partitions considered: " + totalPartitions);
    System.out.println("Valid partitions: " + validPartitions);
    if (validPartitions == 0) {
      System.out.println("No valid partitions identified.");
      return;
    }

    int totalEdges = result.edges().size();
    for (PartitionEvaluation evaluation : result.partitionEvaluations()) {
      System.out.println("Partition #" + evaluation.partitionIndex() + ":");
      List<Component> components = evaluation.partition().components();
      List<Integer> optionCounts = evaluation.componentOptionCounts();
      for (int i = 0; i < components.size(); i++) {
        var component = components.get(i);
        String signature = BitsetUtils.signature(component.edgeBits(), totalEdges);
        int optionsForComponent = optionCounts.get(i);
        System.out.println(
            "  Component #" + (i + 1) + " [" + signature + "] options=" + optionsForComponent);
      }
      if (options.mode() == Mode.ENUMERATE) {
        List<List<KnownComponent>> tuples = evaluation.decompositionTuples();
        if (tuples.isEmpty()) {
          System.out.println("  Tuples: none");
        } else {
          System.out.println("  Tuples (showing " + tuples.size() + "):");
          for (int i = 0; i < tuples.size(); i++) {
            List<KnownComponent> tuple = tuples.get(i);
            List<String> fragments = new ArrayList<>(tuple.size());
            for (KnownComponent component : tuple) {
              fragments.add(
                  component.cpq().toString()
                      + " ("
                      + component.source()
                      + "→"
                      + component.target()
                      + ")");
            }
            System.out.println("    #" + (i + 1) + ": " + String.join(" | ", fragments));
          }
          if (options.enumerationLimit() > 0 && tuples.size() >= options.enumerationLimit()) {
            System.out.println("    ... limit reached (" + options.enumerationLimit() + ")");
          }
        }
      }
    }
  }

  private void exportForVisualization(DecompositionResult result) {
    try {
      var cwd = Paths.get("").toAbsolutePath().normalize();
      Path projectRoot = Files.exists(cwd.resolve("settings.gradle")) ? cwd : cwd.getParent();
      if (projectRoot == null) {
        projectRoot = cwd;
      }
      Path baseDir = projectRoot.resolve("app").resolve("temp");
      VisualizationExporter.export(
          baseDir,
          result.edges(),
          result.freeVariables(),
          result.allPartitions(),
          result.filteredPartitionList(),
          result.cpqPartitions());
    } catch (IOException ex) {
      System.err.println("Warning: failed to export visualization artifacts: " + ex.getMessage());
    }
  }

  private CliOptions parseArgs(String[] args) {
    if (args == null || args.length == 0) {
      throw new IllegalArgumentException("Missing command. Use: decompose [options]");
    }
    String command = args[0];
    if ("decompose".equalsIgnoreCase(command)) {
      return parseDecomposeArgs(args);
    }
    if ("example".equalsIgnoreCase(command)) {
      if (args.length < 2) {
        throw new IllegalArgumentException(
            "Missing example name. Usage: example <exampleName> [options]");
      }
      String[] remapped = remapExampleArgs(args[1], Arrays.copyOfRange(args, 2, args.length));
      return parseDecomposeArgs(remapped);
    }
    if (isExampleAlias(command)) {
      String[] remapped = remapExampleArgs(command, Arrays.copyOfRange(args, 1, args.length));
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
    String maxCoversRaw = null;
    String timeBudgetRaw = null;
    String outputPath = null;
    String limitRaw = null;
    boolean show = false;
    String randomFreeRaw = null;
    String randomEdgesRaw = null;
    String randomLabelsRaw = null;
    String randomSeedRaw = null;

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
        case "--cq" -> queryText = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--cpq" -> cpqText = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--file" ->
            queryFile = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--example" ->
            exampleName = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--free-vars" ->
            freeVarsRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--mode" -> modeRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--max-partitions" ->
            maxPartitionsRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--max-covers" ->
            maxCoversRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--time-budget-ms" ->
            timeBudgetRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--limit" ->
            limitRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--out" ->
            outputPath = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--show" -> show = true;
        case "--random-free-vars" ->
            randomFreeRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--random-edges" ->
            randomEdgesRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--random-labels" ->
            randomLabelsRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        case "--random-seed" ->
            randomSeedRaw = inlineValue != null ? inlineValue : nextValue(args, ++i, option);
        default -> throw new IllegalArgumentException("Unknown option: " + rawArg);
      }
    }

    int sources = 0;
    if (queryText != null) sources++;
    if (cpqText != null) sources++;
    if (queryFile != null) sources++;
    if (exampleName != null) sources++;
    if (sources != 1) {
      throw new IllegalArgumentException(
          "Provide exactly one of --cq, --cpq, --file, or --example");
    }

    Set<String> freeVars = parseFreeVariables(freeVarsRaw);

    Mode mode = Mode.VALIDATE;
    if (modeRaw != null) {
      String normalized = modeRaw.toLowerCase(Locale.ROOT);
      switch (normalized) {
        case "validate" -> mode = Mode.VALIDATE;
        case "enumerate" -> mode = Mode.ENUMERATE;
        case "both", "decompose" -> {
          mode = Mode.ENUMERATE;
          System.err.println(
              "Warning: --mode " + modeRaw + " is deprecated; using --mode enumerate.");
        }
        case "partitions" -> {
          mode = Mode.VALIDATE;
          System.err.println(
              "Warning: --mode " + modeRaw + " is deprecated; using --mode validate.");
        }
        default -> throw new IllegalArgumentException("Invalid mode: " + modeRaw);
      }
    }

    int maxPartitions =
        parseInt(
            maxPartitionsRaw, DecompositionOptions.defaults().maxPartitions(), "--max-partitions");
    int maxCovers =
        parseInt(maxCoversRaw, DecompositionOptions.defaults().maxCovers(), "--max-covers");
    long timeBudget =
        parseLong(
            timeBudgetRaw, DecompositionOptions.defaults().timeBudgetMs(), "--time-budget-ms");
    int limit = parseInt(limitRaw, DecompositionOptions.defaults().enumerationLimit(), "--limit");
    boolean randomOptionsProvided =
        randomFreeRaw != null
            || randomEdgesRaw != null
            || randomLabelsRaw != null
            || randomSeedRaw != null;
    if (randomOptionsProvided && !isRandomExampleName(exampleName)) {
      throw new IllegalArgumentException("--random-* options require --example random");
    }

    RandomExampleConfig randomExampleConfig = null;
    if (isRandomExampleName(exampleName)) {
      int freeVariables =
          parseInt(randomFreeRaw, RandomExampleConfig.DEFAULT_FREE_VARIABLES, "--random-free-vars");
      int edges =
          parseInt(randomEdgesRaw, RandomExampleConfig.DEFAULT_EDGE_COUNT, "--random-edges");
      int labels =
          parseInt(
              randomLabelsRaw, RandomExampleConfig.DEFAULT_PREDICATE_LABELS, "--random-labels");
      Long seed = randomSeedRaw != null ? parseLong(randomSeedRaw, 0L, "--random-seed") : null;
      randomExampleConfig = new RandomExampleConfig(freeVariables, edges, labels, seed);
    }

    return new CliOptions(
        queryText,
        cpqText,
        queryFile,
        exampleName,
        freeVars,
        mode,
        maxPartitions,
        maxCovers,
        timeBudget,
        limit,
        show,
        outputPath,
        randomExampleConfig);
  }

  private boolean isExampleAlias(String command) {
    if (command == null) {
      return false;
    }
    String normalized = command.trim().toLowerCase(Locale.ROOT);
    return normalized.startsWith("example") && normalized.length() > "example".length();
  }

  private boolean isRandomExampleName(String exampleName) {
    if (exampleName == null) {
      return false;
    }
    String normalized = exampleName.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("random") || normalized.equals("examplerandom");
  }

  private String[] remapExampleArgs(String exampleName, String[] remaining) {
    String[] remapped = new String[remaining.length + 3];
    remapped[0] = "decompose";
    remapped[1] = "--example";
    remapped[2] = exampleName;
    System.arraycopy(remaining, 0, remapped, 3, remaining.length);
    return remapped;
  }

  private String nextValue(String[] args, int index, String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException("Missing value for " + option);
    }
    return args[index];
  }

  private Set<String> parseFreeVariables(String raw) {
    if (raw == null || raw.isBlank()) {
      return Set.of();
    }
    Set<String> vars = new LinkedHashSet<>();
    Arrays.stream(raw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(vars::add);
    return vars;
  }

  private int parseInt(String raw, int defaultValue, String optionName) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid integer for " + optionName + ": " + raw);
    }
  }

  private long parseLong(String raw, long defaultValue, String optionName) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid long for " + optionName + ": " + raw);
    }
  }

  private CQ loadQuery(CliOptions options) throws IOException {
    if (options.hasExample()) {
      return loadExample(options);
    }
    if (options.hasQueryText()) {
      return CQ.parse(options.queryText());
    }
    if (options.hasCpqExpression()) {
      String sanitized = options.cpqExpression().replaceAll("\\s+", "");
      return CPQ.parse(sanitized).toCQ();
    }
    if (options.hasQueryFile()) {
      Path path = Path.of(options.queryFile());
      if (!Files.exists(path)) {
        throw new IllegalArgumentException("Query file not found: " + path);
      }
      String content = Files.readString(path);
      return CQ.parse(content);
    }
    throw new IllegalStateException("Missing query input.");
  }

  private CQ loadExample(CliOptions options) {
    String exampleName = options.exampleName();
    String normalized = exampleName.trim().toLowerCase(Locale.ROOT);
    return switch (normalized) {
      case "example1" -> Example.example1();
      case "example2" -> Example.example2();
      case "example3" -> Example.example3();
      case "example4" -> Example.example4();
      case "example5" -> Example.example5();
      case "example6" -> Example.example6();
      case "example7" -> Example.example7();
      case "example8" -> Example.example8();
      case "random", "examplerandom" -> {
        RandomExampleConfig config =
            options.randomExampleConfig() != null
                ? options.randomExampleConfig()
                : RandomExampleConfig.defaults();
        yield Example.random(config);
      }
      default -> throw new IllegalArgumentException("Unknown example: " + exampleName);
    };
  }

  private void writeOutput(String outputPath, String json) throws IOException {
    Path path = Path.of(outputPath);
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(path, json);
  }
}
