package decomposition.cli;

import decomposition.DecompositionOptions;
import decomposition.DecompositionOptions.Mode;
import decomposition.DecompositionPipeline;
import decomposition.DecompositionResult;
import decomposition.cpq.KnownComponent;
import decomposition.model.Partition;
import decomposition.util.VisualizationExporter;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.util.graph.GraphPanel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * CLI entry point for the CQ to CPQ decomposition pipeline.
 */
public final class Main {

    public static void main(String[] args) {
        int exitCode = new Main().run(args);
        System.exit(exitCode);
    }

    int run(String[] args) {
        try {
            CliOptions cliOptions = parseArgs(args);
            DecompositionOptions pipelineOptions = new DecompositionOptions(
                    cliOptions.mode(),
                    cliOptions.maxPartitions(),
                    cliOptions.maxCovers(),
                    cliOptions.timeBudgetMs());

            CQ query = loadQuery(cliOptions);

            DecompositionPipeline pipeline = new DecompositionPipeline();
            DecompositionResult result = pipeline.execute(query, cliOptions.freeVariables(), pipelineOptions);

            exportForVisualization(result);

            JsonReportBuilder reportBuilder = new JsonReportBuilder();
            String json = reportBuilder.build(result);

            if (cliOptions.outputPath() != null) {
                writeOutput(cliOptions.outputPath(), json);
            } else {
                System.out.println(json);
            }

            if (cliOptions.showVisualization()) {
                for (KnownComponent component : result.recognizedComponents()) {
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
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return 4;
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
        if (!"decompose".equalsIgnoreCase(args[0])) {
            throw new IllegalArgumentException("Unknown command: " + args[0]);
        }

        String queryText = null;
        String queryFile = null;
        String freeVarsRaw = null;
        String modeRaw = null;
        String maxPartitionsRaw = null;
        String maxCoversRaw = null;
        String timeBudgetRaw = null;
        String outputPath = null;
        boolean show = false;

        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--cq" -> queryText = nextValue(args, ++i, arg);
                case "--file" -> queryFile = nextValue(args, ++i, arg);
                case "--free-vars" -> freeVarsRaw = nextValue(args, ++i, arg);
                case "--mode" -> modeRaw = nextValue(args, ++i, arg);
                case "--max-partitions" -> maxPartitionsRaw = nextValue(args, ++i, arg);
                case "--max-covers" -> maxCoversRaw = nextValue(args, ++i, arg);
                case "--time-budget-ms" -> timeBudgetRaw = nextValue(args, ++i, arg);
                case "--out" -> outputPath = nextValue(args, ++i, arg);
                case "--show" -> show = true;
                default -> throw new IllegalArgumentException("Unknown option: " + arg);
            }
        }

        if ((queryText == null) == (queryFile == null)) {
            throw new IllegalArgumentException("Provide exactly one of --cq or --file");
        }

        Set<String> freeVars = parseFreeVariables(freeVarsRaw);

        Mode mode = Mode.DECOMPOSE;
        if (modeRaw != null) {
            try {
                mode = Mode.valueOf(modeRaw.toUpperCase());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid mode: " + modeRaw);
            }
        }

        int maxPartitions = parseInt(maxPartitionsRaw, DecompositionOptions.defaults().maxPartitions(), "--max-partitions");
        int maxCovers = parseInt(maxCoversRaw, DecompositionOptions.defaults().maxCovers(), "--max-covers");
        long timeBudget = parseLong(timeBudgetRaw, DecompositionOptions.defaults().timeBudgetMs(), "--time-budget-ms");

        return new CliOptions(queryText, queryFile, freeVars, mode, maxPartitions, maxCovers, timeBudget, show, outputPath);
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
        Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(vars::add);
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
        if (options.hasQueryText()) {
            return CQ.parse(options.queryText());
        }
        Path path = Path.of(options.queryFile());
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Query file not found: " + path);
        }
        String content = Files.readString(path);
        return CQ.parse(content);
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
