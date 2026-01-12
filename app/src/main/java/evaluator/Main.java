package evaluator;

import evaluator.cq.ConjunctiveQuery;
import evaluator.decompose.CpqDecomposition;
import evaluator.index.CpqNativeIndex;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: [indexFile] <cqText...>");
            System.err.println("   or: [indexFile] --queries-file <path>");
            System.err.println("If indexFile is omitted, uses ./index.bin");
            System.err.println("Example: indices/robotssmall.k2.idx \"(x,y) \u2190 0(x,y)\"");
            System.err.println("Example: indices/robotssmall.k2.idx --queries-file queries/robotssmall.cq");
            return;
        }

        ParsedArgs parsed = ParsedArgs.parse(args);
        try {
            CpqNativeIndex index = CpqNativeIndex.load(parsed.indexFile);
            if (parsed.queriesFile != null) {
                evaluateQueriesFile(index, parsed.queriesFile);
            } else {
                ConjunctiveQuery cq = index.parseCQ(parsed.cqText);
                CpqDecomposition decomposition = cq.decompose(index.k());
                List<Map<String, Integer>> answers = index.evaluate(decomposition);

                System.out.println("answers=" + answers.size());
                answers.stream().limit(5).forEach(System.out::println);
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void evaluateQueriesFile(CpqNativeIndex index, Path queriesFile) throws Exception {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(queriesFile, "queriesFile");

        int queryIndex = 0;
        try (BufferedReader reader = Files.newBufferedReader(queriesFile)) {
            for (String line; (line = reader.readLine()) != null;) {
                String query = line.trim();
                if (query.isEmpty() || query.startsWith("#")) {
                    continue;
                }
                queryIndex++;
                ConjunctiveQuery cq = index.parseCQ(query);
                CpqDecomposition decomposition = cq.decompose(index.k());
                List<Map<String, Integer>> answers = index.evaluate(decomposition);
                System.out.println("query=" + queryIndex + " answers=" + answers.size());
            }
        }
        if (queryIndex == 0) {
            System.out.println("No queries found in " + queriesFile);
        }
    }

    private record ParsedArgs(Path indexFile, String cqText, Path queriesFile) {
        static ParsedArgs parse(String[] args) {
            Path defaultIndex = Path.of("index.bin");
            if (args.length == 0) {
                throw new IllegalArgumentException("args must not be empty");
            }

            Path first = Path.of(args[0]);
            if (Files.exists(first) && !Files.isDirectory(first)) {
                return parseAfterIndex(first, Arrays.copyOfRange(args, 1, args.length));
            }

            return parseAfterIndex(defaultIndex, args);
        }

        private static ParsedArgs parseAfterIndex(Path indexFile, String[] rest) {
            if (rest.length == 0) {
                throw new IllegalArgumentException("CQ text missing");
            }
            if (rest.length >= 2 && "--queries-file".equals(rest[0])) {
                return new ParsedArgs(indexFile, "", Path.of(rest[1]));
            }
            String cqText = String.join(" ", rest);
            return new ParsedArgs(indexFile, cqText, null);
        }
    }
}
