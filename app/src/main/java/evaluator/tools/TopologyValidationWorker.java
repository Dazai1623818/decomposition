package evaluator.tools;

import evaluator.evaluation.DecompositionMethod;
import evaluator.evaluation.Planner;
import evaluator.index.NativeCpqIndex;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Warm worker that keeps one dataset index loaded and validates query sets for
 * the topology workload builder over a simple stdin job protocol.
 */
public final class TopologyValidationWorker {
    private TopologyValidationWorker() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        NativeCpqIndex index = NativeCpqIndex.load(options.indexPath());
        Planner planner = new Planner(index);
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
                runJob(index, planner, options.timeoutMs(), options.validationMode(), Path.of(command));
                System.out.println("__TOPOLOGY_VALIDATION_DONE__");
                System.out.flush();
            }
        }
    }

    private static void runJob(
            NativeCpqIndex index,
            Planner planner,
            int timeoutMs,
            TopologyDiverseWorkloadBuilder.ValidationMode validationMode,
            Path jobFile) throws Exception {
        Properties properties = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(jobFile, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        Path queriesFile = Path.of(requiredProperty(properties, "queries_file"));
        Path resultFile = Path.of(requiredProperty(properties, "result_file"));

        List<String> results = new ArrayList<>();
        for (String query : loadQueries(queriesFile)) {
            results.add(TopologyDiverseWorkloadBuilder.evaluateQuery(
                    query,
                    index,
                    planner,
                    DecompositionMethod.MAX_COLLAPSE,
                    timeoutMs,
                    validationMode).serialize());
        }
        Files.write(resultFile, results, StandardCharsets.UTF_8);
    }

    private static List<String> loadQueries(Path queriesFile) throws Exception {
        List<String> queries = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(queriesFile, StandardCharsets.UTF_8)) {
            for (String line; (line = reader.readLine()) != null;) {
                String query = line.trim();
                if (query.isEmpty() || query.startsWith("#")) {
                    continue;
                }
                queries.add(query);
            }
        }
        return List.copyOf(queries);
    }

    private static String requiredProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing topology validation worker property: " + key);
        }
        return value;
    }

    private record Options(
            Path indexPath,
            int timeoutMs,
            TopologyDiverseWorkloadBuilder.ValidationMode validationMode) {
        private static Options parse(String[] args) {
            Objects.requireNonNull(args, "args");
            Path indexPath = null;
            int timeoutMs = 0;
            TopologyDiverseWorkloadBuilder.ValidationMode validationMode =
                    TopologyDiverseWorkloadBuilder.ValidationMode.FIRST_ANSWER;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--index" -> indexPath = Path.of(requireValue(args, ++i, "--index"));
                    case "--timeout-ms" -> timeoutMs = Integer.parseInt(requireValue(args, ++i, "--timeout-ms"));
                    case "--validation-mode" -> validationMode = TopologyDiverseWorkloadBuilder.ValidationMode.parse(
                            requireValue(args, ++i, "--validation-mode"));
                    default -> throw new IllegalArgumentException("Unknown option: " + arg);
                }
            }
            if (indexPath == null) {
                throw new IllegalArgumentException("Missing required argument: --index");
            }
            if (timeoutMs <= 0) {
                throw new IllegalArgumentException("--timeout-ms must be > 0");
            }
            return new Options(indexPath, timeoutMs, validationMode);
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value after " + option);
            }
            return args[index];
        }
    }
}
