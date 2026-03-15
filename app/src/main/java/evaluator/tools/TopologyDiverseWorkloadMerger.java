package evaluator.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * Merges topology workload shard outputs produced by {@link TopologyDiverseWorkloadBuilder}.
 */
public final class TopologyDiverseWorkloadMerger {
    private static final int[] DEFAULT_SCALES = new int[] { 10, 20, 50 };

    private TopologyDiverseWorkloadMerger() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        new TopologyDiverseWorkloadMerger().run(options);
    }

    private void run(Options options) throws IOException {
        Files.createDirectories(options.outputDir());
        List<Path> shardDirs = sortShardDirs(options.shardDirs());
        if (shardDirs.isEmpty()) {
            throw new IllegalArgumentException("No shard directories provided");
        }
        mergeReadme(shardDirs, options.outputDir(), options.scales());
        mergeAcceptanceSummary(shardDirs, options.outputDir());
        mergeCandidatePool(shardDirs, options.outputDir());
        for (int scale : options.scales()) {
            mergeScaleOutputs(shardDirs, options.outputDir(), scale);
        }
    }

    private static List<Path> sortShardDirs(List<Path> shardDirs) {
        List<Path> ordered = new ArrayList<>(shardDirs);
        ordered.sort(Comparator.comparingInt(TopologyDiverseWorkloadMerger::firstWorkloadId));
        return List.copyOf(ordered);
    }

    private static int firstWorkloadId(Path shardDir) {
        Path summary = shardDir.resolve("acceptance_summary.csv");
        try (BufferedReader reader = Files.newBufferedReader(summary, StandardCharsets.UTF_8)) {
            reader.readLine();
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return Integer.MAX_VALUE;
            }
            int comma = line.indexOf(',');
            return Integer.parseInt(comma < 0 ? line : line.substring(0, comma));
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read " + summary, ex);
        }
    }

    private static void mergeReadme(List<Path> shardDirs, Path outputDir, int[] scales) throws IOException {
        List<String> firstReadme = Files.readAllLines(shardDirs.get(0).resolve("README.txt"), StandardCharsets.UTF_8);
        int templates = countSummaryRows(shardDirs);
        long acceptedBodies = sumReadmeValue(shardDirs, "accepted_bodies");
        long evaluatedBodies = sumReadmeValue(shardDirs, "evaluated_bodies");
        long eligibleBodies = sumReadmeValue(shardDirs, "eligible_bodies");
        long rejectedBodies = sumReadmeValue(shardDirs, "rejected_bodies");

        List<String> lines = new ArrayList<>();
        lines.add("mode=merged_shards");
        lines.add("merged_at=" + Instant.now());
        lines.add("shards=" + shardDirs.size());
        lines.add("templates=" + templates);
        lines.add("scales=" + joinInts(scales));
        lines.add("accepted_bodies=" + acceptedBodies);
        lines.add("evaluated_bodies=" + evaluatedBodies);
        lines.add("eligible_bodies=" + eligibleBodies);
        lines.add("rejected_bodies=" + rejectedBodies);
        for (int scale : scales) {
            lines.add("templates_meeting_i" + scale + "=" + sumScaleCoverage(shardDirs, scale) + "/" + templates);
        }
        for (String line : firstReadme) {
            if (line.startsWith("templates=")
                    || line.startsWith("template_start=")
                    || line.startsWith("template_count=")
                    || line.startsWith("scales=")
                    || line.startsWith("accepted_bodies=")
                    || line.startsWith("evaluated_bodies=")
                    || line.startsWith("eligible_bodies=")
                    || line.startsWith("rejected_bodies=")
                    || line.startsWith("templates_meeting_i")) {
                continue;
            }
            lines.add(line);
        }
        for (int i = 0; i < shardDirs.size(); i++) {
            lines.add(String.format(Locale.ROOT, "shard[%d]=%s", i, shardDirs.get(i).toAbsolutePath()));
        }
        Files.write(outputDir.resolve("README.txt"), lines, StandardCharsets.UTF_8);
    }

    private static int countSummaryRows(List<Path> shardDirs) throws IOException {
        int total = 0;
        for (Path shardDir : shardDirs) {
            try (BufferedReader reader = Files.newBufferedReader(shardDir.resolve("acceptance_summary.csv"),
                    StandardCharsets.UTF_8)) {
                reader.readLine();
                for (String line; (line = reader.readLine()) != null;) {
                    if (!line.isBlank()) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    private static long sumReadmeValue(List<Path> shardDirs, String key) throws IOException {
        long total = 0L;
        for (Path shardDir : shardDirs) {
            List<String> lines = Files.readAllLines(shardDir.resolve("README.txt"), StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.startsWith(key + "=")) {
                    total += Long.parseLong(line.substring(key.length() + 1));
                }
            }
        }
        return total;
    }

    private static void mergeAcceptanceSummary(List<Path> shardDirs, Path outputDir) throws IOException {
        mergeCsvByConcatenation(shardDirs, outputDir.resolve("acceptance_summary.csv"), "acceptance_summary.csv", false);
    }

    private static void mergeCandidatePool(List<Path> shardDirs, Path outputDir) throws IOException {
        mergeCsvByConcatenation(shardDirs, outputDir.resolve("candidate_pool.csv"), "candidate_pool.csv", false);
    }

    private static void mergeScaleOutputs(List<Path> shardDirs, Path outputDir, int scale) throws IOException {
        String prefix = String.format(Locale.ROOT, "topology-bench.a123_i%d", scale);
        mergeTextFiles(shardDirs, outputDir.resolve(prefix + ".cq"), prefix + ".cq");
        mergeCsvByConcatenation(shardDirs, outputDir.resolve(prefix + ".body_validation.csv"),
                prefix + ".body_validation.csv", false);
        mergeQueryMap(shardDirs, outputDir.resolve(prefix + ".query_map.csv"), prefix + ".query_map.csv");
    }

    private static void mergeTextFiles(List<Path> shardDirs, Path outputFile, String fileName) throws IOException {
        List<String> merged = new ArrayList<>();
        for (Path shardDir : shardDirs) {
            merged.addAll(Files.readAllLines(shardDir.resolve(fileName), StandardCharsets.UTF_8));
        }
        Files.write(outputFile, merged, StandardCharsets.UTF_8);
    }

    private static void mergeCsvByConcatenation(
            List<Path> shardDirs,
            Path outputFile,
            String fileName,
            boolean renumberQueryId) throws IOException {
        List<String> merged = new ArrayList<>();
        boolean headerWritten = false;
        long nextQueryId = 1L;
        for (Path shardDir : shardDirs) {
            List<String> lines = Files.readAllLines(shardDir.resolve(fileName), StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                continue;
            }
            if (!headerWritten) {
                merged.add(lines.get(0));
                headerWritten = true;
            }
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.isBlank()) {
                    continue;
                }
                if (renumberQueryId) {
                    int comma = line.indexOf(',');
                    line = nextQueryId + line.substring(comma);
                    nextQueryId++;
                }
                merged.add(line);
            }
        }
        Files.write(outputFile, merged, StandardCharsets.UTF_8);
    }

    private static void mergeQueryMap(List<Path> shardDirs, Path outputFile, String fileName) throws IOException {
        mergeCsvByConcatenation(shardDirs, outputFile, fileName, true);
    }

    private static long sumScaleCoverage(List<Path> shardDirs, int scale) throws IOException {
        return sumReadmeValue(shardDirs, "templates_meeting_i" + scale, '/');
    }

    private static long sumReadmeValue(List<Path> shardDirs, String key, char until) throws IOException {
        long total = 0L;
        for (Path shardDir : shardDirs) {
            List<String> lines = Files.readAllLines(shardDir.resolve("README.txt"), StandardCharsets.UTF_8);
            for (String line : lines) {
                if (!line.startsWith(key + "=")) {
                    continue;
                }
                String value = line.substring(key.length() + 1);
                int stop = value.indexOf(until);
                total += Long.parseLong(stop >= 0 ? value.substring(0, stop) : value);
            }
        }
        return total;
    }

    private static String joinInts(int[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(values[i]);
        }
        return builder.toString();
    }

    private record Options(
            Path outputDir,
            List<Path> shardDirs,
            int[] scales) {
        private static Options parse(String[] args) {
            Path outputDir = null;
            List<Path> shardDirs = List.of();
            int[] scales = Arrays.copyOf(DEFAULT_SCALES, DEFAULT_SCALES.length);
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--output-dir" -> outputDir = Path.of(requireValue(args, ++i, "--output-dir"));
                    case "--shard-dirs" -> shardDirs = parsePaths(requireValue(args, ++i, "--shard-dirs"));
                    case "--scales" -> scales = parseScales(requireValue(args, ++i, "--scales"));
                    default -> throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            if (outputDir == null) {
                throw new IllegalArgumentException("--output-dir is required");
            }
            if (shardDirs.isEmpty()) {
                throw new IllegalArgumentException("--shard-dirs is required");
            }
            return new Options(outputDir, shardDirs, scales);
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " requires a value");
            }
            return args[index];
        }

        private static List<Path> parsePaths(String raw) {
            LinkedHashSet<Path> paths = new LinkedHashSet<>();
            for (String part : raw.split(",")) {
                String value = part.trim();
                if (!value.isEmpty()) {
                    paths.add(Path.of(value));
                }
            }
            return List.copyOf(paths);
        }

        private static int[] parseScales(String raw) {
            String[] parts = raw.split(",");
            int[] parsed = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                parsed[i] = Integer.parseInt(parts[i].trim());
            }
            Arrays.sort(parsed);
            return parsed;
        }
    }
}
