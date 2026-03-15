package evaluator.cli;

import evaluator.bench.BenchRunner;
import evaluator.bench.BenchTypes;
import evaluator.bench.EngineConfig;
import evaluator.evaluation.DecompositionMethod;
import evaluator.index.NativeCpqIndex;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

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
        if (args.command() == Command.COMPARE_FILE_WORKER) {
            runCompareFileWorker(index, args);
            return;
        }
        EngineConfig runConfig = SYSTEM_CONFIG
                .withEstimationSeed(args.seed())
                .withSystemRMaxCandidateOrders(args.joinOrderBudget());
        BenchRunner runner = new BenchRunner(index, runConfig);

        switch (args.command()) {
            case EVAL_FILE -> runEvalFile(runner, args);
            case EXPLORE -> runExplore(runner, args);
            case COMPARE -> runCompare(runner, args);
            case COMPARE_FILE -> runCompareFile(runner, args);
            case COMPARE_FILE_WORKER -> throw new IllegalStateException("compare-file-worker handled earlier");
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
                requiredMethod(args.method(), "--method"),
                args.coverLimit(),
                args.kOverride(),
                args.timeoutMs(),
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
                        "query=%d method=%s comps=%d max_diam=%d answers=%d parse_ms=%.3f planning_ms=%.3f index_lookup_ms=%.3f mapping_ms=%.3f join_order_ms=%.3f join_ms=%.3f execution_ms=%.3f method_wall_ms=%.3f end_to_end_ms=%.3f status=%s%s",
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
                        nanosToMillis(progress.queryNanos()
                                + progress.mappingNanos()
                                + progress.estimateNanos()
                                + progress.joinNanos()),
                        nanosToMillis(Math.max(0L, progress.wallNanos() - progress.parseNanos())),
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
                0,
                args.profileOrders(),
                args.seed());
        BenchTypes.ExploreReport report = runner.explore(spec);
        System.out.println(String.format(
                Locale.ROOT,
                "command=explore candidates=%d status=%s",
                report.candidateCount(),
                report.status().name()));
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
        System.out.println(String.format(
                Locale.ROOT,
                "command=compare evaluation_method=%s candidates=%d timeouts=%d status=%s",
                evaluationMethodId,
                report.comparedMethods(),
                report.timeoutCount(),
                report.status().name()));
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
                args.warmupQueriesFile(),
                args.warmupQueryLimit(),
                args.mode(),
                args.methodTimeoutMs(),
                args.decompositionTimeoutMs(),
                args.coverLimit(),
                args.kOverride(),
                args.seed(),
                compareLog,
                decompositionLog,
                args.warmup());
        BenchTypes.CompareFileReport report = runner.compareFile(spec);
        String status = (report.timeoutRows() == 0 && report.decompositionTimeoutRows() == 0 && report.errorRows() == 0)
                ? "OK"
                : "PARTIAL";
        System.out.println(String.format(
                Locale.ROOT,
                "command=compare-file queries=%d method_rows=%d ok=%d exec_timeouts=%d planning_timeouts=%d no_candidate=%d errors=%d elapsed_ms=%.3f status=%s",
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

    private static void runCompareFileWorker(NativeCpqIndex index, ParsedArgs args) throws Exception {
        WorkerRunnerState runnerState = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String command = line.trim();
                if (command.isEmpty()) {
                    continue;
                }
                if ("STOP".equals(command)) {
                    return;
                }

                WorkerJob job = WorkerJob.load(Path.of(command));
                runnerState = ensureWorkerRunner(index, job, runnerState);
                runWorkerJob(runnerState.runner(), job);
                System.out.println("DONE");
                System.out.flush();
            }
        }
    }

    private static WorkerRunnerState ensureWorkerRunner(
            NativeCpqIndex index,
            WorkerJob job,
            WorkerRunnerState current) {
        if (current != null
                && Objects.equals(current.methodsProperty(), job.methodsProperty())
                && current.seed() == job.seed()
                && current.joinOrderBudget() == job.joinOrderBudget()) {
            return current;
        }

        if (job.methodsProperty() == null || job.methodsProperty().isBlank()) {
            System.clearProperty("cpq.decompose.methods");
        } else {
            System.setProperty("cpq.decompose.methods", job.methodsProperty());
        }
        EngineConfig runConfig = SYSTEM_CONFIG
                .withEstimationSeed(job.seed())
                .withSystemRMaxCandidateOrders(job.joinOrderBudget());
        return new WorkerRunnerState(
                job.methodsProperty(),
                job.seed(),
                job.joinOrderBudget(),
                new BenchRunner(index, runConfig));
    }

    private static void runWorkerJob(BenchRunner runner, WorkerJob job) throws Exception {
        Files.createDirectories(job.runDir());
        deleteIfExists(job.runDir().resolve("exit.status"));
        deleteIfExists(job.runDir().resolve("error.txt"));

        try {
            BenchTypes.CompareFileSpec spec = new BenchTypes.CompareFileSpec(
                    job.indexPath(),
                    job.queriesFile(),
                    job.warmupQueriesFile(),
                    job.warmupQueryLimit(),
                    job.mode(),
                    job.methodTimeoutMs(),
                    job.decompositionTimeoutMs(),
                    job.coverLimit(),
                    job.kOverride(),
                    job.seed(),
                    job.compareLogPath(),
                    job.decompositionLogPath(),
                    job.warmup());
            runner.compareFile(spec);
            Files.writeString(job.runDir().resolve("exit.status"), "0\n", StandardCharsets.UTF_8);
        } catch (Exception ex) {
            writeWorkerError(job.runDir().resolve("error.txt"), ex);
            Files.writeString(job.runDir().resolve("exit.status"), "1\n", StandardCharsets.UTF_8);
        }
    }

    private static void deleteIfExists(Path path) throws Exception {
        Files.deleteIfExists(path);
    }

    private static void writeWorkerError(Path path, Exception ex) throws Exception {
        StringWriter buffer = new StringWriter();
        try (PrintWriter writer = new PrintWriter(buffer)) {
            ex.printStackTrace(writer);
        }
        Files.writeString(path, buffer.toString(), StandardCharsets.UTF_8);
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
                args.seed(),
                outputPath);
        BenchTypes.EstimationBenchReport report = runner.estimationBench(spec);
        String status = (report.timeoutRows() == 0 && report.decompositionTimeoutRows() == 0 && report.errorRows() == 0)
                ? "OK"
                : "PARTIAL";
        System.out.println(String.format(
                Locale.ROOT,
                "command=estimationbench queries=%d method_rows=%d step_rows=%d ok=%d exec_timeouts=%d planning_timeouts=%d no_candidate=%d errors=%d elapsed_ms=%.3f output=%s status=%s",
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
                requiredMethod(args.method(), "--method"),
                args.coverLimit(),
                args.kOverride(),
                args.profileOrders(),
                args.seed(),
                args.timeoutMs());
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
                requiredMethod(args.method(), "--method"),
                args.coverLimit(),
                args.kOverride(),
                args.timeoutMs());
        BenchTypes.EstimateReport report = runner.estimate(spec);
        String status = report.timedOut() ? "TIMEOUT" : "OK";
        System.out.println(String.format(
                Locale.ROOT,
                "command=estimate estimate=%.6f stderr=%.6f status=%s",
                report.estimate(),
                report.standardError(),
                status));
        if (args.outputDir() != null) {
            writeEstimateOutputs(args.outputDir(), report);
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
                        "{\"candidate_count\":%d,\"decomposition_methods\":\"%s\",\"status\":\"%s\"}%n",
                        report.candidateCount(),
                        supportedDecompositionMethods(),
                        report.status().name()),
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
                        report.status().name()),
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

    private static void writeEstimateOutputs(Path outputDir, BenchTypes.EstimateReport report)
            throws Exception {
        Files.createDirectories(outputDir);
        Files.writeString(
                outputDir.resolve("estimate.jsonl"),
                String.format(
                        Locale.ROOT,
                        "{\"estimate\":%.6f,\"standard_error\":%.6f,\"status\":\"%s\"}%n",
                        report.estimate(),
                        report.standardError(),
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

    private static DecompositionMethod requiredMethod(DecompositionMethod method, String name) {
        if (method == null) {
            throw new IllegalArgumentException(
                    "Missing required argument: " + name + " (supported: " + supportedDecompositionMethods() + ")");
        }
        return method;
    }

    private static void printUsage() {
        System.err.println("Usage: <command> [options]");
        System.err.println("Commands:");
        System.err.println("  eval-file --queries-file <path> [--index <path>]");
        System.err.println("  compare-file --queries-file <path> [--index <path>]");
        System.err.println("  compare-file-worker --index <path>");
        System.err.println("  estimationbench --queries-file <path> [--index <path>]");
        System.err.println("  explore <query> [--index <path>]");
        System.err.println("  compare <query> [--index <path>]");
        System.err.println("  profile <query> [--index <path>]");
        System.err.println("  estimate <query> [--index <path>]");
        System.err.println("Common options:");
        System.err.println("  --index <path>                      (default: index.bin)");
        System.err.println("  --output-dir <path>                write command output files");
        System.err.println("  --method <id>                      required for eval-file/profile/estimate");
        System.err.println("  --rows | --count                   evaluation mode (default: rows)");
        System.err.println("  --cover-limit <n> --k <n> --candidate-limit <n>");
        System.err.println("  --timeout-ms <n>                   single-plan timeout for eval-file/profile/estimate");
        System.err.println("  --per-method-timeout-ms <n> --planning-timeout-ms <n>");
        System.err.println("  --join-order-budget <n> --profile-orders <n> --seed <n>");
        System.err.println("  --min-components <n> --max-components <n> --warmup-rounds <n>");
        System.err.println("  --warmup                            run compare-file warmup workload before measuring");
        System.err.println("  --warmup-query-limit <n>            limit compare-file warmup workload size");
        System.err.println("  --warmup-queries-file <path>         warmup workload for compare-file or estimationbench");
        System.err.println("  --compare-log <path> --decomposition-log <path>");
    }

    private enum Command {
        EVAL_FILE("eval-file"),
        COMPARE_FILE("compare-file"),
        COMPARE_FILE_WORKER("compare-file-worker"),
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
            DecompositionMethod method,
            int coverLimit,
            int candidateLimit,
            int kOverride,
            boolean warmup,
            int warmupQueryLimit,
            int warmupRounds,
            int timeoutMs,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int joinOrderBudget,
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
            BenchTypes.EvaluationMode mode = BenchTypes.EvaluationMode.ROWS;
            DecompositionMethod method = null;
            int coverLimit = 2048;
            int candidateLimit = 0;
            int kOverride = 0;
            boolean warmup = false;
            int warmupQueryLimit = 0;
            int warmupRounds = 0;
            int timeoutMs = 0;
            int methodTimeoutMs = 0;
            int decompositionTimeoutMs = 0;
            int joinOrderBudget = SYSTEM_CONFIG.systemRMaxCandidateOrders();
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
                    case "--method" -> method = DecompositionMethod.fromToken(requireValue(args, ++i, "--method"));
                    case "--rows" -> mode = BenchTypes.EvaluationMode.ROWS;
                    case "--count", "--count-only" -> mode = BenchTypes.EvaluationMode.COUNT;
                    case "--cover-limit" -> coverLimit = parseInt(requireValue(args, ++i, "--cover-limit"), arg);
                    case "--candidate-limit" -> candidateLimit = parseInt(requireValue(args, ++i, "--candidate-limit"), arg);
                    case "--k" -> kOverride = parseInt(requireValue(args, ++i, "--k"), arg);
                    case "--warmup" -> warmup = true;
                    case "--warmup-query-limit" -> warmupQueryLimit = parseInt(requireValue(args, ++i, "--warmup-query-limit"), arg);
                    case "--warmup-rounds" -> warmupRounds = parseInt(requireValue(args, ++i, "--warmup-rounds"), arg);
                    case "--timeout-ms", "--profile-timeout-ms" -> timeoutMs = parseInt(requireValue(args, ++i, arg), arg);
                    case "--per-method-timeout-ms", "--method-timeout-ms" ->
                        methodTimeoutMs = parseInt(requireValue(args, ++i, arg), arg);
                    case "--planning-timeout-ms", "--decomposition-timeout-ms" ->
                        decompositionTimeoutMs = parseInt(requireValue(args, ++i, arg), arg);
                    case "--join-order-budget" ->
                        joinOrderBudget = parseInt(requireValue(args, ++i, "--join-order-budget"), arg);
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

            validate(
                    command,
                    queriesFile,
                    queryText,
                    method,
                    joinOrderBudget,
                    minComponents,
                    maxComponents,
                    warmup,
                    warmupQueryLimit);
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
                    method,
                    coverLimit,
                    candidateLimit,
                    kOverride,
                    warmup,
                    warmupQueryLimit,
                    warmupRounds,
                    timeoutMs,
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    joinOrderBudget,
                    profileOrders,
                    minComponents,
                    maxComponents,
                    seed);
        }

        private static void validate(
                Command command,
                Path queriesFile,
                String queryText,
                DecompositionMethod method,
                int joinOrderBudget,
                int minComponents,
                int maxComponents,
                boolean warmup,
                int warmupQueryLimit) {
            if (command != Command.COMPARE_FILE && warmupQueryLimit != 0) {
                throw new IllegalArgumentException("--warmup-query-limit is only supported for compare-file");
            }
            if (joinOrderBudget < 0) {
                throw new IllegalArgumentException("join-order-budget must be >= 0");
            }
            if (warmupQueryLimit < 0) {
                throw new IllegalArgumentException("warmup-query-limit must be >= 0");
            }
            if (minComponents < 0 || maxComponents < 0) {
                throw new IllegalArgumentException("min-components and max-components must be >= 0");
            }
            if (minComponents > 0 && maxComponents > 0 && minComponents > maxComponents) {
                throw new IllegalArgumentException("min-components must be <= max-components");
            }
            if (warmup && command != Command.COMPARE_FILE) {
                throw new IllegalArgumentException("--warmup is only supported for compare-file");
            }
            if (command == Command.COMPARE_FILE_WORKER) {
                return;
            }
            if (command == Command.EVAL_FILE
                    || command == Command.COMPARE_FILE
                    || command == Command.ESTIMATION_BENCH) {
                if (queriesFile == null) {
                    throw new IllegalArgumentException(command.token + " requires --queries-file <path>");
                }
                if (command == Command.EVAL_FILE && method == null) {
                    throw new IllegalArgumentException(command.token + " requires --method <id>");
                }
                return;
            }
            if ((command == Command.PROFILE || command == Command.ESTIMATE) && method == null) {
                throw new IllegalArgumentException(command.token + " requires --method <id>");
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

    private record WorkerRunnerState(
            String methodsProperty,
            long seed,
            int joinOrderBudget,
            BenchRunner runner) {
    }

    private record WorkerJob(
            Path runDir,
            Path indexPath,
            Path queriesFile,
            Path compareLogPath,
            Path decompositionLogPath,
            Path warmupQueriesFile,
            int warmupQueryLimit,
            BenchTypes.EvaluationMode mode,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int kOverride,
            long seed,
            String methodsProperty,
            int joinOrderBudget,
            boolean warmup) {
        private static WorkerJob load(Path jobFile) throws Exception {
            Properties properties = new Properties();
            try (BufferedReader reader = Files.newBufferedReader(jobFile, StandardCharsets.UTF_8)) {
                properties.load(reader);
            }
            Path runDir = jobFile.toAbsolutePath().getParent();
            Path warmupQueriesFile = propertyPath(properties, "warmup_queries_file");
            String modeToken = properties.getProperty("mode", BenchTypes.EvaluationMode.ROWS.id());
            return new WorkerJob(
                    runDir,
                    Path.of(requiredProperty(properties, "index_path")),
                    Path.of(requiredProperty(properties, "query_file")),
                    Path.of(requiredProperty(properties, "compare_log_file")),
                    Path.of(requiredProperty(properties, "decomposition_log_file")),
                    warmupQueriesFile,
                    intProperty(properties, "warmup_query_limit", 0),
                    "count".equals(modeToken)
                            ? BenchTypes.EvaluationMode.COUNT
                            : BenchTypes.EvaluationMode.ROWS,
                    intProperty(properties, "method_timeout_ms", 0),
                    intProperty(properties, "decomposition_timeout_ms", 0),
                    intProperty(properties, "cover_limit", 0),
                    intProperty(properties, "k_override", 0),
                    longProperty(properties, "seed", 0L),
                    properties.getProperty("methods_property", ""),
                    intProperty(properties, "join_order_budget", SYSTEM_CONFIG.systemRMaxCandidateOrders()),
                    booleanProperty(properties, "warmup", false));
        }

        private static String requiredProperty(Properties properties, String key) {
            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing worker job property: " + key);
            }
            return value;
        }

        private static Path propertyPath(Properties properties, String key) {
            String value = properties.getProperty(key);
            return value == null || value.isBlank() ? null : Path.of(value);
        }

        private static int intProperty(Properties properties, String key, int fallback) {
            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                return fallback;
            }
            return Integer.parseInt(value.trim());
        }

        private static long longProperty(Properties properties, String key, long fallback) {
            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                return fallback;
            }
            return Long.parseLong(value.trim());
        }

        private static boolean booleanProperty(Properties properties, String key, boolean fallback) {
            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                return fallback;
            }
            return Boolean.parseBoolean(value.trim());
        }
    }
}
