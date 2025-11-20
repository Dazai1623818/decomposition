package decomposition.cli;

import decomposition.DecompositionOptions;
import decomposition.RandomExampleConfig;
import decomposition.cpq.model.CacheStats;
import decomposition.profile.PipelineProfiler;
import decomposition.profile.PipelineProfiler.NamedQuery;
import decomposition.profile.PipelineProfiler.PipelineProfile;
import decomposition.profile.PipelineProfiler.ProfileRun;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the `profile` command. */
final class ProfileCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileCommand.class);

  int execute(String[] args) {
    ProfileOptions options = parseProfileArgs(args);
    PipelineProfiler profiler = new PipelineProfiler();
    PipelineProfile report = profiler.profile(options.queries(), options.options());
    logProfileReport(report, options.options());
    return 0;
  }

  @SuppressWarnings("StringSplitter")
  private ProfileOptions parseProfileArgs(String[] args) {
    if (args.length == 0 || !"profile".equalsIgnoreCase(args[0])) {
      throw new IllegalArgumentException("Unknown command: " + (args.length == 0 ? "" : args[0]));
    }

    String examplesRaw = null;
    String modeRaw = null;
    String maxPartitionsRaw = null;
    String timeBudgetRaw = null;
    String limitRaw = null;
    boolean singleTuple = false;
    String randomCountRaw = null;
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
        case "--examples" ->
            examplesRaw =
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
        case "--single-tuple" -> singleTuple = true;
        case "--random-count" ->
            randomCountRaw =
                inlineValue != null ? inlineValue : CliParsers.nextValue(args, ++i, option);
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

    DecompositionOptions.Mode mode = DecompositionOptions.Mode.VALIDATE;
    if (modeRaw != null) {
      String normalized = modeRaw.toLowerCase(Locale.ROOT);
      switch (normalized) {
        case "validate" -> mode = DecompositionOptions.Mode.VALIDATE;
        case "enumerate" -> mode = DecompositionOptions.Mode.ENUMERATE;
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
    int randomCount = CliParsers.parseInt(randomCountRaw, 0, "--random-count");
    boolean randomOptionsProvided =
        randomFreeRaw != null
            || randomEdgesRaw != null
            || randomLabelsRaw != null
            || randomSeedRaw != null;

    RandomExampleConfig baseRandomConfig = null;
    if (randomCount > 0 || randomOptionsProvided) {
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
      baseRandomConfig = new RandomExampleConfig(freeVariables, edges, labels, seed);
    }

    List<NamedQuery> queries = new ArrayList<>();
    int randomIndex = 1;
    if (examplesRaw == null || examplesRaw.isBlank()) {
      queries.addAll(PipelineProfiler.defaultExamples());
    } else {
      for (String token : examplesRaw.split(",")) {
        String trimmed = token.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        String normalized = trimmed.toLowerCase(Locale.ROOT);
        if (CliParsers.isRandomExampleName(normalized)) {
          RandomExampleConfig config =
              CliParsers.iterationRandomConfig(baseRandomConfig, randomIndex - 1);
          CQ query = CliParsers.loadExampleByName(normalized, config);
          queries.add(NamedQuery.of("random-" + randomIndex, query));
          randomIndex++;
        } else {
          CQ query = CliParsers.loadExampleByName(normalized, null);
          queries.add(NamedQuery.of(trimmed, query));
        }
      }
    }

    for (int i = 0; i < randomCount; i++) {
      RandomExampleConfig config =
          CliParsers.iterationRandomConfig(baseRandomConfig, randomIndex - 1);
      queries.add(PipelineProfiler.randomQuery("random-" + randomIndex, config));
      randomIndex++;
    }

    DecompositionOptions options =
        new DecompositionOptions(mode, maxPartitions, timeBudget, limit, singleTuple, false);
    return new ProfileOptions(queries, options);
  }

  private void logProfileReport(PipelineProfile report, DecompositionOptions options) {
    LOG.info(
        "Profiling {} queries with options: mode={}, maxPartitions={}, timeBudgetMs={}, enumerationLimit={}",
        report.runs().size(),
        options.mode(),
        options.maxPartitions(),
        options.timeBudgetMs(),
        options.enumerationLimit());

    for (ProfileRun run : report.runs()) {
      LOG.info(
          "- {}: elapsed={}ms, partitions={}, filtered={}, valid={}, recognized={}",
          run.queryName(),
          run.elapsedMillis(),
          run.totalPartitions(),
          run.filteredPartitions(),
          run.validPartitions(),
          run.recognizedComponents());
      CacheStats cache = run.cacheSnapshot();
      if (cache != null && cache.lookups() > 0) {
        double hitRatePct = cache.hitRate() * 100.0;
        String pct = String.format(Locale.ROOT, "%.2f", hitRatePct);
        LOG.info(
            "  cache: lookups={} hits={} misses={} hitRate={}%%",
            cache.lookups(), cache.hits(), cache.misses(), pct);
      } else {
        LOG.info("  cache: no lookups");
      }
      if (run.terminationReason() != null) {
        LOG.warn("  termination: {}", run.terminationReason());
      }
    }

    CacheStats aggregate = report.aggregateCache();
    double aggregatePct = aggregate.hitRate() * 100.0;
    String pct = String.format(Locale.ROOT, "%.2f", aggregatePct);
    LOG.info(
        "Aggregate cache: lookups={} hits={} misses={} hitRate={}%%",
        aggregate.lookups(), aggregate.hits(), aggregate.misses(), pct);
    LOG.info("Total elapsed millis: {}", report.totalElapsedMillis());
  }
}
