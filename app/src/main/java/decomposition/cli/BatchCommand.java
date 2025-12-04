package decomposition.cli;

import com.google.common.base.Splitter;
import decomposition.core.DecompositionOptions;
import decomposition.eval.IndexManager;
import decomposition.pipeline.Pipeline;
import decomposition.pipeline.PlanMode;
import decomposition.util.Timing;
import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cq.CQ;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes a batch of example decompositions against many graphs in one JVM (shared index). */
final class BatchCommand {
  private static final Logger LOG = LoggerFactory.getLogger(BatchCommand.class);
  private static final List<String> DEFAULT_EXAMPLES =
      IntStream.rangeClosed(1, 8).mapToObj(i -> "example" + i).toList();
  private static final int DEFAULT_INDEX_K = 2;

  int execute(String[] args) throws IOException {
    BatchOptions options = parseArgs(args);
    List<Path> graphs = resolveGraphs(options.graphsPath());
    if (graphs.isEmpty()) {
      LOG.warn("No .edge graphs found under {}.", options.graphsPath());
      return 1;
    }

    Files.createDirectories(options.logsDir());
    Pipeline pipeline = new Pipeline();

    boolean hadFailure = false;
    for (Path graph : graphs) {
      try {
        runForGraph(graph, pipeline, options);
      } catch (RuntimeException ex) {
        hadFailure = true;
        LOG.error("Batch run failed for {}: {}", graph, ex.getMessage(), ex);
      }
    }
    return hadFailure ? 1 : 0;
  }

  private void runForGraph(Path graph, Pipeline pipeline, BatchOptions options) throws IOException {
    int effectiveK = options.indexK() > 0 ? options.indexK() : DEFAULT_INDEX_K;
    Timing indexTimer = Timing.start();
    Index index = new IndexManager().loadOrBuild(graph, effectiveK);
    long indexLoadMs = indexTimer.elapsedMillis();
    LOG.info("Loaded index for {} in {} ms (k={})", graph.getFileName(), indexLoadMs, effectiveK);

    DecompositionOptions pipelineOptions = buildOptions(options);
    String baseName = baseName(graph);
    Path logFile =
        options.logsDir().resolve(planLabel(options.planMode()) + "-" + baseName + ".txt");

    PrintStream originalOut = System.out;
    try (OutputStream fileOut =
            Files.newOutputStream(
                logFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        TeePrintStream tee = new TeePrintStream(fileOut, originalOut)) {
      System.setOut(tee);
      System.out.printf(
          "== Starting batch for %s (%s) ==%n",
          graph.getFileName(), options.examples().stream().collect(Collectors.joining(", ")));

      for (String example : options.examples()) {
        runExample(
            pipeline, pipelineOptions, graph, index, effectiveK, indexLoadMs, baseName, example);
      }
    } finally {
      System.setOut(originalOut);
    }
  }

  private void runExample(
      Pipeline pipeline,
      DecompositionOptions pipelineOptions,
      Path graph,
      Index index,
      int effectiveK,
      long indexLoadMs,
      String graphBaseName,
      String exampleName)
      throws IOException {
    System.out.printf("== %s %s ==%n", graphBaseName, exampleName);
    CQ query = CliParsers.loadExampleByName(exampleName);
    pipeline.benchmarkWithIndex(
        query, Set.of(), pipelineOptions, graph, index, effectiveK, indexLoadMs);
    System.out.println();
  }

  private BatchOptions parseArgs(String[] args) {
    String[] effectiveArgs = stripCommand(args);
    Map<String, OptionSpec> specs = optionSpecs();
    BatchOptions.Builder builder = BatchOptions.builder();

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

  private String[] stripCommand(String[] args) {
    if (args == null || args.length == 0) {
      return new String[0];
    }
    String first = args[0];
    if ("batch".equalsIgnoreCase(first) || "batch-random".equalsIgnoreCase(first)) {
      return Arrays.copyOfRange(args, 1, args.length);
    }
    return args;
  }

  private Map<String, OptionSpec> optionSpecs() {
    Map<String, OptionSpec> specs = new LinkedHashMap<>();
    specs.put("--graphs", OptionSpec.withValue((b, raw) -> b.graphsPath(Path.of(raw))));
    specs.put("--log-dir", OptionSpec.withValue((b, raw) -> b.logsDir(Path.of(raw))));
    specs.put("--examples", OptionSpec.withValue((b, raw) -> b.examples(parseExamples(raw))));
    specs.put("--plan", OptionSpec.withValue((b, raw) -> b.planMode(parsePlanMode(raw))));
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
    specs.put(
        "--tuple-limit",
        OptionSpec.withValue(
            (b, raw) ->
                b.tupleLimit(
                    CliParsers.parseInt(
                        raw, DecompositionOptions.defaults().tupleLimit(), "--tuple-limit"))));
    specs.put(
        "--k",
        OptionSpec.withValue(
            (b, raw) -> b.indexK(CliParsers.parseInt(raw, DEFAULT_INDEX_K, "--k"))));
    return specs;
  }

  private PlanMode parsePlanMode(String raw) {
    if (raw == null || raw.isBlank()) {
      return PlanMode.RANDOM;
    }
    return switch (raw.toLowerCase(Locale.ROOT)) {
      case "single", "single-edge", "edges" -> PlanMode.SINGLE_EDGE;
      case "first", "first-valid" -> PlanMode.FIRST;
      case "random", "rand" -> PlanMode.RANDOM;
      case "all", "default" -> PlanMode.ALL;
      default -> throw new IllegalArgumentException("Invalid plan: " + raw);
    };
  }

  private List<String> parseExamples(String raw) {
    if (raw == null || raw.isBlank() || "all".equalsIgnoreCase(raw.trim())) {
      return DEFAULT_EXAMPLES;
    }

    List<String> tokens =
        Splitter.on(',')
            .trimResults()
            .omitEmptyStrings()
            .splitToStream(raw)
            .map(this::normalizeExampleName)
            .toList();
    if (tokens.isEmpty()) {
      throw new IllegalArgumentException("No examples provided.");
    }
    return List.copyOf(tokens);
  }

  private String normalizeExampleName(String raw) {
    String lower = raw.toLowerCase(Locale.ROOT);
    if (lower.matches("\\d+")) {
      return "example" + lower;
    }
    if (lower.startsWith("example")) {
      return lower;
    }
    return "example" + lower;
  }

  private List<Path> resolveGraphs(Path graphsRoot) throws IOException {
    Path normalized = graphsRoot.toAbsolutePath().normalize();
    if (Files.isRegularFile(normalized)) {
      return List.of(normalized);
    }
    if (!Files.isDirectory(normalized)) {
      throw new IllegalArgumentException("Graph path is not a directory or file: " + normalized);
    }
    try (Stream<Path> stream = Files.list(normalized)) {
      return stream
          .filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".edge"))
          .sorted()
          .toList();
    }
  }

  private DecompositionOptions buildOptions(BatchOptions options) {
    int effectiveTupleLimit = options.tupleLimit() > 0 ? options.tupleLimit() : 1;
    return new DecompositionOptions(
        DecompositionOptions.Mode.ENUMERATE,
        options.maxPartitions(),
        options.timeBudgetMs(),
        effectiveTupleLimit,
        false,
        options.planMode(),
        options.indexK());
  }

  private String baseName(Path graphPath) {
    String fileName = graphPath.getFileName().toString();
    int dot = fileName.lastIndexOf('.');
    return dot > 0 ? fileName.substring(0, dot) : fileName;
  }

  private String planLabel(PlanMode planMode) {
    return planMode.name().toLowerCase(Locale.ROOT).replace('_', '-');
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

  private record OptionSpec(boolean requiresValue, BiConsumer<BatchOptions.Builder, String> apply) {
    static OptionSpec withValue(BiConsumer<BatchOptions.Builder, String> consumer) {
      return new OptionSpec(true, consumer);
    }

    void apply(BatchOptions.Builder builder, String value) {
      apply.accept(builder, value);
    }
  }

  private static final class TeePrintStream extends PrintStream {
    private final PrintStream mirror;

    TeePrintStream(OutputStream fileOut, PrintStream mirror) throws IOException {
      super(fileOut, true, StandardCharsets.UTF_8);
      this.mirror = Objects.requireNonNull(mirror, "mirror");
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      super.write(buf, off, len);
      mirror.write(buf, off, len);
    }

    @Override
    public void flush() {
      super.flush();
      mirror.flush();
    }
  }

  private record BatchOptions(
      Path graphsPath,
      Path logsDir,
      List<String> examples,
      PlanMode planMode,
      int maxPartitions,
      long timeBudgetMs,
      int tupleLimit,
      int indexK) {

    BatchOptions {
      graphsPath = graphsPath == null ? Path.of("graphs") : graphsPath;
      logsDir = logsDir == null ? Path.of("logs") : logsDir;
      examples = examples == null || examples.isEmpty() ? DEFAULT_EXAMPLES : List.copyOf(examples);
      planMode = planMode == null ? PlanMode.RANDOM : planMode;
      maxPartitions =
          maxPartitions > 0 ? maxPartitions : DecompositionOptions.defaults().maxPartitions();
      timeBudgetMs = Math.max(0, timeBudgetMs);
      tupleLimit = tupleLimit > 0 ? tupleLimit : DecompositionOptions.defaults().tupleLimit();
      indexK = indexK > 0 ? indexK : DEFAULT_INDEX_K;
    }

    static Builder builder() {
      return new Builder();
    }

    static final class Builder {
      private Path graphsPath = Path.of("graphs");
      private Path logsDir = Path.of("logs");
      private List<String> examples = DEFAULT_EXAMPLES;
      private PlanMode planMode = PlanMode.RANDOM;
      private int maxPartitions = DecompositionOptions.defaults().maxPartitions();
      private long timeBudgetMs = DecompositionOptions.defaults().timeBudgetMs();
      private int tupleLimit = DecompositionOptions.defaults().tupleLimit();
      private int indexK = DEFAULT_INDEX_K;

      Builder graphsPath(Path graphsPath) {
        this.graphsPath = graphsPath;
        return this;
      }

      Builder logsDir(Path logsDir) {
        this.logsDir = logsDir;
        return this;
      }

      Builder examples(List<String> examples) {
        if (examples != null && !examples.isEmpty()) {
          this.examples = List.copyOf(examples);
        }
        return this;
      }

      Builder planMode(PlanMode planMode) {
        this.planMode = planMode;
        return this;
      }

      Builder maxPartitions(int maxPartitions) {
        this.maxPartitions = maxPartitions;
        return this;
      }

      Builder timeBudgetMs(long timeBudgetMs) {
        this.timeBudgetMs = timeBudgetMs;
        return this;
      }

      Builder tupleLimit(int tupleLimit) {
        this.tupleLimit = tupleLimit;
        return this;
      }

      Builder indexK(int indexK) {
        this.indexK = indexK;
        return this;
      }

      BatchOptions build() {
        return new BatchOptions(
            graphsPath,
            logsDir,
            examples,
            planMode,
            maxPartitions,
            timeBudgetMs,
            tupleLimit,
            indexK);
      }
    }
  }
}
