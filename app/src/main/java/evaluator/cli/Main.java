package evaluator.cli;

import evaluator.bench.BenchRunner;
import evaluator.bench.BenchTypes;
import evaluator.bench.EngineConfig;
import evaluator.evaluation.DecompositionMethod;
import evaluator.index.NativeCpqIndex;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

public final class Main {
    private static final int EXIT_USAGE = 2;
    private static final Path DEFAULT_INDEX = Path.of("index.bin");
    private static final EngineConfig SYSTEM_CONFIG = EngineConfig.fromSystemProperties();

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0]) || "help".equals(args[0])) {
            printUsage();
            return;
        }

        ParsedArgs parsed;
        try {
            parsed = ParsedArgs.parse(args);
        } catch (IllegalArgumentException ex) {
            System.err.println(ex.getMessage());
            printUsage();
            System.exit(EXIT_USAGE);
            return;
        }

        try {
            run(parsed);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(ParsedArgs args) throws Exception {
        NativeCpqIndex index = NativeCpqIndex.load(args.indexPath());
        EngineConfig runConfig = SYSTEM_CONFIG.withEstimationSeed(args.seed());
        BenchRunner runner = new BenchRunner(index, runConfig);

        switch (args.command()) {
            case EVAL_FILE -> runEvalFile(runner, args);
            case EXPLORE -> runExplore(runner, args);
            case COMPARE -> runCompare(runner, args);
            case COMPARE_FILE -> runCompareFile(runner, args);
            case ESTIMATION_BENCH -> runEstimationBench(runner, args);
            case PROFILE -> runProfile(runner, args);
            case ESTIMATE -> runEstimate(runner, args);
        }
    }

    private static void runEvalFile(BenchRunner runner, ParsedArgs args) throws Exception {
        BenchTypes.EvalFileProgressSink progressSink = stdoutEvalFileProgressSink();
        BenchTypes.EvalFileSpec spec = new BenchTypes.EvalFileSpec(
                args.indexPath(),
                requiredPath(args.queriesFile(), "--queries-file"),
                args.mode(),
                args.coverLimit(),
                args.kOverride(),
                args.candidateLimit(),
                args.warmupRounds(),
                1,
                args.methodTimeoutMs(),
                args.decompositionTimeoutMs(),
                args.profileTimeoutMs(),
                args.estimateWalks(),
                args.minComponents(),
                args.maxComponents(),
                args.seed(),
                args.outputDir(),
                progressSink);
        BenchTypes.EvalFileReport report = runner.evalFile(spec);
        String evaluationMethodId = BenchTypes.EvaluationMethod.fromMode(args.mode()).id();
        String status = report.timeoutCount() > 0
                ? "TIMEOUT"
                : (report.failureCount() == 0 ? "OK" : "PARTIAL");
        System.out.println(String.format(
                Locale.ROOT,
                "command=eval-file evaluation_method=%s seed=%d queries=%d failures=%d timeouts=%d status=%s",
                evaluationMethodId,
                args.seed(),
                report.queryCount(),
                report.failureCount(),
                report.timeoutCount(),
                status));
        if (args.outputDir() != null) {
            writeEvalFileOutputs(args.outputDir(), report, evaluationMethodId, args.seed());
        }
    }

    private static BenchTypes.EvalFileProgressSink stdoutEvalFileProgressSink() {
        return new BenchTypes.EvalFileProgressSink() {
            private static final int PROGRESS_EVERY = 25;

            private final long startedNanos = System.nanoTime();
            private int completedQueries;

            @Override
            public void onQueryResult(BenchTypes.EvalFileProgress progress) {
                String errorSegment = progress.errorMessage() == null || progress.errorMessage().isBlank()
                        ? ""
                        : String.format(Locale.ROOT, " error=\"%s\"", sanitize(progress.errorMessage()));
                String methodId = progress.decompositionMethodId() == null
                        ? "-"
                        : progress.decompositionMethodId();
                System.out.println(String.format(
                        Locale.ROOT,
                        "query=%d method=%s comps=%d max_diam=%d answers=%d parse_ms=%.3f decompose_ms=%.3f query_ms=%.3f mapping_ms=%.3f estimate_ms=%.3f join_ms=%.3f wall_ms=%.3f status=%s%s",
                        progress.queryNumber(),
                        methodId,
                        progress.components(),
                        progress.maxDiameter(),
                        progress.answers(),
                        nanosToMillis(progress.parseNanos()),
                        nanosToMillis(progress.decomposeNanos()),
                        nanosToMillis(progress.queryNanos()),
                        nanosToMillis(progress.mappingNanos()),
                        nanosToMillis(progress.estimateNanos()),
                        nanosToMillis(progress.joinNanos()),
                        nanosToMillis(progress.wallNanos()),
                        progress.status(),
                        errorSegment));
                completedQueries++;
                if (completedQueries % PROGRESS_EVERY == 0) {
                    System.out.println(String.format(
                            Locale.ROOT,
                            "progress_time=%s queries_completed=%d elapsed_ms=%.3f",
                            Instant.now(),
                            completedQueries,
                            nanosToMillis(System.nanoTime() - startedNanos)));
                }
            }
        };
    }

    private static String sanitize(String value) {
        return value.replace('"', '\'');
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static void runExplore(BenchRunner runner, ParsedArgs args) throws Exception {
        BenchTypes.ExploreSpec spec = new BenchTypes.ExploreSpec(
                args.indexPath(),
                requiredText(args.queryText(), "<query>"),
                args.coverLimit(),
                args.kOverride(),
                args.decompositionTimeoutMs(),
                args.candidateLimit(),
                args.minComponents(),
                args.maxComponents(),
                args.methodTimeoutMs(),
                args.profileTimeoutMs(),
                args.profileOrders(),
                args.estimateWalks(),
                args.seed());
        BenchTypes.ExploreReport report = runner.explore(spec);
        System.out.println(String.format(
                Locale.ROOT,
                "command=explore candidates=%d status=OK",
                report.candidateCount()));
        if (args.outputDir() != null) {
            writeExploreOutputs(args.outputDir(), report);
        }
    }

    private static void runCompare(BenchRunner runner, ParsedArgs args) throws Exception {
        BenchTypes.CompareSpec spec = new BenchTypes.CompareSpec(
                args.indexPath(),
                requiredText(args.queryText(), "<query>"),
                args.mode(),
                args.coverLimit(),
                args.kOverride(),
                args.decompositionTimeoutMs(),
                args.methodTimeoutMs(),
                args.seed());
        BenchTypes.CompareReport report = runner.compare(spec);
        String evaluationMethodId = BenchTypes.EvaluationMethod.fromMode(args.mode()).id();
        String status = report.timeoutCount() > 0 ? "TIMEOUT" : "OK";
        System.out.println(String.format(
                Locale.ROOT,
                "command=compare evaluation_method=%s candidates=%d timeouts=%d status=%s",
                evaluationMethodId,
                report.comparedMethods(),
                report.timeoutCount(),
                status));
        if (args.outputDir() != null) {
            writeCompareOutputs(args.outputDir(), report, evaluationMethodId);
        }
    }

    private static void runCompareFile(BenchRunner runner, ParsedArgs args) throws Exception {
        Path compareLog = args.compareLogPath();
        Path decompositionLog = args.decompositionLogPath();
        if (args.outputDir() != null) {
            if (compareLog == null) {
                compareLog = args.outputDir().resolve("compare_file.log");
            }
            if (decompositionLog == null) {
                decompositionLog = args.outputDir().resolve("decompositions.log");
            }
        }

        BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                args.indexPath(),
                requiredPath(args.queriesFile(), "--queries-file"),
                args.methodTimeoutMs(),
                args.decompositionTimeoutMs(),
                args.coverLimit(),
                args.kOverride(),
                args.seed(),
                compareLog,
                decompositionLog);
        BenchTypes.CompareFileReport report = runner.compareFile(spec);
        String status = (report.timeoutRows() == 0 && report.decompositionTimeoutRows() == 0 && report.errorRows() == 0)
                ? "OK"
                : "PARTIAL";
        System.out.println(String.format(
                Locale.ROOT,
                "command=compare-file queries=%d method_rows=%d ok=%d timeouts=%d decomp_timeouts=%d no_candidate=%d errors=%d elapsed_ms=%.3f status=%s",
                report.queryCount(),
                report.methodRows(),
                report.okRows(),
                report.timeoutRows(),
                report.decompositionTimeoutRows(),
                report.noCandidateRows(),
                report.errorRows(),
                nanosToMillis(report.elapsedNanos()),
                status));
    }

    private static void runEstimationBench(BenchRunner runner, ParsedArgs args) throws Exception {
        Path outputPath = args.compareLogPath();
        if (args.outputDir() != null && outputPath == null) {
            outputPath = args.outputDir().resolve("estimationbench.log");
        }
        if (outputPath == null) {
            outputPath = Path.of("logs", "estimationbench.log");
        }
        BenchTypes.EstimationBenchSpec spec = new BenchTypes.EstimationBenchSpec(
                args.indexPath(),
                requiredPath(args.queriesFile(), "--queries-file"),
                args.warmupQueriesFile(),
                args.methodTimeoutMs(),
                args.decompositionTimeoutMs(),
                args.coverLimit(),
                args.kOverride(),
                args.estimateWalks() > 0 ? args.estimateWalks() : 64,
                args.seed(),
                outputPath);
        BenchTypes.EstimationBenchReport report = runner.estimationBench(spec);
        String status = (report.timeoutRows() == 0 && report.decompositionTimeoutRows() == 0 && report.errorRows() == 0)
                ? "OK"
                : "PARTIAL";
        System.out.println(String.format(
                Locale.ROOT,
                "command=estimationbench queries=%d method_rows=%d step_rows=%d ok=%d timeouts=%d decomp_timeouts=%d no_candidate=%d errors=%d elapsed_ms=%.3f output=%s status=%s",
                report.queryCount(),
                report.methodRows(),
                report.stepRows(),
                report.okRows(),
                report.timeoutRows(),
                report.decompositionTimeoutRows(),
                report.noCandidateRows(),
                report.errorRows(),
                nanosToMillis(report.elapsedNanos()),
                outputPath,
                status));
    }

    private static void runProfile(BenchRunner runner, ParsedArgs args) throws Exception {
        BenchTypes.ProfileSpec spec = new BenchTypes.ProfileSpec(
                args.indexPath(),
                requiredText(args.queryText(), "<query>"),
                args.profileOrders(),
                args.seed(),
                args.profileTimeoutMs());
        BenchTypes.ProfileReport report = runner.profile(spec);
        String status = report.timedOut() ? "TIMEOUT" : "OK";
        System.out.println(String.format(
                Locale.ROOT,
                "command=profile order_policy=%s profiled_orders=%d status=%s",
                BenchTypes.OrderPolicy.RANDOM_SAMPLE.id(),
                report.profiledOrders(),
                status));
        if (args.outputDir() != null) {
            writeProfileOutputs(args.outputDir(), report);
        }
    }

    private static void runEstimate(BenchRunner runner, ParsedArgs args) throws Exception {
        BenchTypes.EstimateSpec spec = new BenchTypes.EstimateSpec(
                args.indexPath(),
                requiredText(args.queryText(), "<query>"),
                args.coverLimit(),
                args.kOverride(),
                args.decompositionTimeoutMs(),
                args.methodTimeoutMs(),
                args.estimateWalks() > 0 ? args.estimateWalks() : 64,
                args.seed());
        BenchTypes.EstimateReport report = runner.estimate(spec);
        String status = report.timedOut() ? "TIMEOUT" : "OK";
        System.out.println(String.format(
                Locale.ROOT,
                "command=estimate estimate=%.6f stderr=%.6f seed=%d walks=%d status=%s",
                report.estimate(),
                report.standardError(),
                args.seed(),
                spec.walks(),
                status));
        if (args.outputDir() != null) {
            writeEstimateOutputs(args.outputDir(), report, args.seed(), spec.walks());
        }
    }

    private static void writeEvalFileOutputs(
            Path outputDir,
            BenchTypes.EvalFileReport report,
            String evaluationMethodId,
            long seed) throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("results.jsonl"),
                String.format(
                        Locale.ROOT,
                        "{\"query_count\":%d,\"failure_count\":%d,\"timeout_count\":%d,\"evaluation_method_id\":\"%s\",\"seed\":%d,\"status\":\"%s\"}%n",
                        report.queryCount(),
                        report.failureCount(),
                        report.timeoutCount(),
                        evaluationMethodId,
                        seed,
                        report.timeoutCount() > 0 ? "TIMEOUT" : (report.failureCount() == 0 ? "OK" : "PARTIAL")),
                StandardCharsets.UTF_8);
        Files.writeString(
                outputDir.resolve("summary.csv"),
                String.format(
                        Locale.ROOT,
                        "query_count,failure_count,timeout_count,evaluation_method_id,seed,status%n%d,%d,%d,%s,%d,%s%n",
                        report.queryCount(),
                        report.failureCount(),
                        report.timeoutCount(),
                        evaluationMethodId,
                        seed,
                        report.timeoutCount() > 0 ? "TIMEOUT" : (report.failureCount() == 0 ? "OK" : "PARTIAL")),
                StandardCharsets.UTF_8);
    }

    private static void writeExploreOutputs(Path outputDir, BenchTypes.ExploreReport report) throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("candidates.jsonl"),
                String.format(
                        Locale.ROOT,
                        "{\"candidate_count\":%d,\"decomposition_methods\":\"%s\",\"status\":\"OK\"}%n",
                        report.candidateCount(),
                        supportedDecompositionMethods()),
                StandardCharsets.UTF_8);
    }

    private static void writeCompareOutputs(
            Path outputDir,
            BenchTypes.CompareReport report,
            String evaluationMethodId) throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("compare.csv"),
                String.format(
                        Locale.ROOT,
                        "evaluation_method_id,compared_candidates,timeout_count,status%n%s,%d,%d,%s%n",
                        evaluationMethodId,
                        report.comparedMethods(),
                        report.timeoutCount(),
                        report.timeoutCount() > 0 ? "TIMEOUT" : "OK"),
                StandardCharsets.UTF_8);
    }

    private static void writeProfileOutputs(Path outputDir, BenchTypes.ProfileReport report) throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("profile_orders.csv"),
                String.format(
                        Locale.ROOT,
                        "order_policy_id,profiled_orders,status%n%s,%d,%s%n",
                        BenchTypes.OrderPolicy.RANDOM_SAMPLE.id(),
                        report.profiledOrders(),
                        report.timedOut() ? "TIMEOUT" : "OK"),
                StandardCharsets.UTF_8);
    }

    private static void writeEstimateOutputs(Path outputDir, BenchTypes.EstimateReport report, long seed, int walks)
            throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("estimate.jsonl"),
                String.format(
                        Locale.ROOT,
                        "{\"estimate\":%.6f,\"standard_error\":%.6f,\"seed\":%d,\"walks\":%d,\"status\":\"%s\"}%n",
                        report.estimate(),
                        report.standardError(),
                        seed,
                        walks,
                        report.timedOut() ? "TIMEOUT" : "OK"),
                StandardCharsets.UTF_8);
    }

    private static String supportedDecompositionMethods() {
        StringBuilder builder = new StringBuilder();
        DecompositionMethod[] methods = DecompositionMethod.values();
        for (int i = 0; i < methods.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(methods[i].id());
        }
        return builder.toString();
    }

    private static String requiredText(String text, String name) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: " + name);
        }
        return text;
    }

    private static Path requiredPath(Path path, String name) {
        if (path == null) {
            throw new IllegalArgumentException("Missing required argument: " + name);
        }
        return path;
    }

    private static void printUsage() {
        System.err.println("Usage: <command> [options]");
        System.err.println("Commands:");
        System.err.println("  eval-file --queries-file <path> [--index <path>]");
        System.err.println("  compare-file --queries-file <path> [--index <path>]");
        System.err.println("  estimationbench --queries-file <path> [--index <path>]");
        System.err.println("  explore <query> [--index <path>]");
        System.err.println("  compare <query> [--index <path>]");
        System.err.println("  profile <query> [--index <path>]");
        System.err.println("  estimate <query> [--index <path>]");
        System.err.println("Common options:");
        System.err.println("  --index <path>                      (default: index.bin)");
        System.err.println("  --output-dir <path>                write command output files");
        System.err.println("  --rows | --count                   evaluation mode (default: count)");
        System.err.println("  --cover-limit <n> --k <n> --candidate-limit <n>");
        System.err.println("  --method-timeout-ms <n> --decomposition-timeout-ms <n> --profile-timeout-ms <n>");
        System.err.println("  --estimate-walks <n> --profile-orders <n> --seed <n>");
        System.err.println("  --min-components <n> --max-components <n> --warmup-rounds <n>");
        System.err.println("  --warmup-queries-file <path>         warmup-only workload for estimationbench");
        System.err.println("  --compare-log <path> --decomposition-log <path>");
    }

    private enum Command {
        EVAL_FILE("eval-file"),
        COMPARE_FILE("compare-file"),
        ESTIMATION_BENCH("estimationbench"),
        EXPLORE("explore"),
        COMPARE("compare"),
        PROFILE("profile"),
        ESTIMATE("estimate");

        private final String token;

        Command(String token) {
            this.token = token;
        }

        static Command parse(String token) {
            for (Command command : values()) {
                if (command.token.equals(token)) {
                    return command;
                }
            }
            throw new IllegalArgumentException("Unknown command: " + token);
        }
    }

    private record ParsedArgs(
            Command command,
            Path indexPath,
            Path queriesFile,
            Path warmupQueriesFile,
            String queryText,
            Path outputDir,
            Path compareLogPath,
            Path decompositionLogPath,
            BenchTypes.EvaluationMode mode,
            int coverLimit,
            int candidateLimit,
            int kOverride,
            int warmupRounds,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int profileTimeoutMs,
            int estimateWalks,
            int profileOrders,
            int minComponents,
            int maxComponents,
            long seed) {
        private static ParsedArgs parse(String[] args) {
            Objects.requireNonNull(args, "args");
            if (args.length == 0) {
                throw new IllegalArgumentException("Missing command");
            }

            Command command = Command.parse(args[0]);
            Path indexPath = DEFAULT_INDEX;
            Path queriesFile = null;
            Path warmupQueriesFile = null;
            String queryText = null;
            Path outputDir = null;
            Path compareLogPath = null;
            Path decompositionLogPath = null;
            BenchTypes.EvaluationMode mode = BenchTypes.EvaluationMode.COUNT;
            int coverLimit = 1;
            int candidateLimit = 0;
            int kOverride = 0;
            int warmupRounds = 0;
            int methodTimeoutMs = 0;
            int decompositionTimeoutMs = 0;
            int profileTimeoutMs = 0;
            int estimateWalks = 0;
            int profileOrders = 0;
            int minComponents = 0;
            int maxComponents = 0;
            long seed = SYSTEM_CONFIG.estimationSeed();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--index" -> indexPath = Path.of(requireValue(args, ++i, "--index"));
                    case "--queries-file" -> queriesFile = Path.of(requireValue(args, ++i, "--queries-file"));
                    case "--warmup-queries-file" -> warmupQueriesFile = Path.of(requireValue(args, ++i, "--warmup-queries-file"));
                    case "--output-dir" -> outputDir = Path.of(requireValue(args, ++i, "--output-dir"));
                    case "--compare-log" -> compareLogPath = Path.of(requireValue(args, ++i, "--compare-log"));
                    case "--decomposition-log" -> decompositionLogPath = Path.of(requireValue(args, ++i, "--decomposition-log"));
                    case "--rows" -> mode = BenchTypes.EvaluationMode.ROWS;
                    case "--count", "--count-only" -> mode = BenchTypes.EvaluationMode.COUNT;
                    case "--cover-limit" -> coverLimit = parseInt(requireValue(args, ++i, "--cover-limit"), arg);
                    case "--candidate-limit" -> candidateLimit = parseInt(requireValue(args, ++i, "--candidate-limit"), arg);
                    case "--k" -> kOverride = parseInt(requireValue(args, ++i, "--k"), arg);
                    case "--warmup-rounds" -> warmupRounds = parseInt(requireValue(args, ++i, "--warmup-rounds"), arg);
                    case "--method-timeout-ms" -> methodTimeoutMs = parseInt(requireValue(args, ++i, "--method-timeout-ms"), arg);
                    case "--decomposition-timeout-ms" -> decompositionTimeoutMs = parseInt(requireValue(args, ++i, "--decomposition-timeout-ms"), arg);
                    case "--profile-timeout-ms" -> profileTimeoutMs = parseInt(requireValue(args, ++i, "--profile-timeout-ms"), arg);
                    case "--estimate-walks" -> estimateWalks = parseInt(requireValue(args, ++i, "--estimate-walks"), arg);
                    case "--profile-orders" -> profileOrders = parseInt(requireValue(args, ++i, "--profile-orders"), arg);
                    case "--min-components" -> minComponents = parseInt(requireValue(args, ++i, "--min-components"), arg);
                    case "--max-components" -> maxComponents = parseInt(requireValue(args, ++i, "--max-components"), arg);
                    case "--seed" -> seed = parseLong(requireValue(args, ++i, "--seed"), arg);
                    default -> {
                        if (arg.startsWith("--")) {
                            throw new IllegalArgumentException("Unknown option: " + arg);
                        }
                        if (queryText == null) {
                            queryText = arg;
                        } else {
                            queryText = queryText + " " + arg;
                        }
                    }
                }
            }

            validate(command, queriesFile, queryText, minComponents, maxComponents);
            return new ParsedArgs(
                    command,
                    indexPath,
                    queriesFile,
                    warmupQueriesFile,
                    queryText,
                    outputDir,
                    compareLogPath,
                    decompositionLogPath,
                    mode,
                    coverLimit,
                    candidateLimit,
                    kOverride,
                    warmupRounds,
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    profileTimeoutMs,
                    estimateWalks,
                    profileOrders,
                    minComponents,
                    maxComponents,
                    seed);
        }

        private static void validate(
                Command command,
                Path queriesFile,
                String queryText,
                int minComponents,
                int maxComponents) {
            if (minComponents < 0 || maxComponents < 0) {
                throw new IllegalArgumentException("min-components and max-components must be >= 0");
            }
            if (minComponents > 0 && maxComponents > 0 && minComponents > maxComponents) {
                throw new IllegalArgumentException("min-components must be <= max-components");
            }
            if (command == Command.EVAL_FILE
                    || command == Command.COMPARE_FILE
                    || command == Command.ESTIMATION_BENCH) {
                if (queriesFile == null) {
                    throw new IllegalArgumentException(command.token + " requires --queries-file <path>");
                }
                return;
            }
            if (queryText == null || queryText.isBlank()) {
                throw new IllegalArgumentException(command.token + " requires a query argument");
            }
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value after " + option);
            }
            return args[index];
        }

        private static int parseInt(String value, String option) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid integer for " + option + ": " + value);
            }
        }

        private static long parseLong(String value, String option) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid long for " + option + ": " + value);
            }
        }
    }
}
