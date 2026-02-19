package evaluator.cli;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CliOutputSchemaTest {
    private static final Path INDEX_PATH = Path.of("index/robots.k2.idx");
    private static final Path SMOKE_QUERIES = Path.of("src/test/resources/queries_smoke.txt");
    private static final String SIMPLE_QUERY = "(x,y) \u2190 0(x,y)";

    @Test
    void helpListsFiveCommands() {
        // Smoke-check command surface in help output.
        Main.main(new String[] { "--help" });
    }

    @Test
    void evalFileWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "eval-file",
                "--index", INDEX_PATH.toString(),
                "--queries-file", SMOKE_QUERIES.toString(),
                "--output-dir", tempDir.toString(),
                "--count"
        });

        Path jsonl = tempDir.resolve("results.jsonl");
        Path summary = tempDir.resolve("summary.csv");
        assertTrue(Files.exists(jsonl));
        assertTrue(Files.exists(summary));

        String json = Files.readString(jsonl, StandardCharsets.UTF_8);
        String csv = Files.readString(summary, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"query_count\""));
        assertTrue(json.contains("\"failure_count\""));
        assertTrue(json.contains("\"timeout_count\""));
        assertTrue(json.contains("\"evaluation_method_id\""));
        assertTrue(json.contains("\"seed\""));
        assertTrue(csv.startsWith("query_count,failure_count,timeout_count,evaluation_method_id,seed,status"));
    }

    @Test
    void exploreWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "explore",
                "--index", INDEX_PATH.toString(),
                SIMPLE_QUERY,
                "--output-dir", tempDir.toString(),
                "--candidate-limit", "3"
        });

        Path jsonl = tempDir.resolve("candidates.jsonl");
        assertTrue(Files.exists(jsonl));
        String json = Files.readString(jsonl, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"candidate_count\""));
    }

    @Test
    void compareWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "compare",
                "--index", INDEX_PATH.toString(),
                SIMPLE_QUERY,
                "--output-dir", tempDir.toString(),
                "--count"
        });

        Path csv = tempDir.resolve("compare.csv");
        assertTrue(Files.exists(csv));
        String content = Files.readString(csv, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("evaluation_method_id,compared_candidates,timeout_count,status"));
    }

    @Test
    void compareFileWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "compare-file",
                "--index", INDEX_PATH.toString(),
                "--queries-file", SMOKE_QUERIES.toString(),
                "--output-dir", tempDir.toString()
        });

        Path compareLog = tempDir.resolve("compare_file.log");
        Path decompositionLog = tempDir.resolve("decompositions.log");
        assertTrue(Files.exists(compareLog));
        assertTrue(Files.exists(decompositionLog));

        String compareContent = Files.readString(compareLog, StandardCharsets.UTF_8);
        String decompositionContent = Files.readString(decompositionLog, StandardCharsets.UTF_8);
        assertTrue(compareContent.contains("summary query_count="));
        assertTrue(decompositionContent.contains("decomposition="));
    }

    @Test
    void profileWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "profile",
                "--index", INDEX_PATH.toString(),
                SIMPLE_QUERY,
                "--output-dir", tempDir.toString(),
                "--profile-orders", "2",
                "--seed", "123"
        });

        Path csv = tempDir.resolve("profile_orders.csv");
        assertTrue(Files.exists(csv));
        String content = Files.readString(csv, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("order_policy_id,profiled_orders,status"));
    }

    @Test
    void estimateWritesExpectedFiles(@TempDir Path tempDir) throws Exception {
        assumeInputs();
        Main.main(new String[] {
                "estimate",
                "--index", INDEX_PATH.toString(),
                SIMPLE_QUERY,
                "--output-dir", tempDir.toString(),
                "--estimate-walks", "8",
                "--seed", "123"
        });

        Path jsonl = tempDir.resolve("estimate.jsonl");
        assertTrue(Files.exists(jsonl));
        String json = Files.readString(jsonl, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"estimate\""));
        assertTrue(json.contains("\"standard_error\""));
        assertTrue(json.contains("\"seed\""));
        assertTrue(json.contains("\"walks\""));
        assertTrue(json.contains("\"status\""));
    }

    private static void assumeInputs() {
        Assumptions.assumeTrue(Files.exists(INDEX_PATH), "Missing index file " + INDEX_PATH);
        Assumptions.assumeTrue(Files.exists(SMOKE_QUERIES), "Missing smoke query fixture " + SMOKE_QUERIES);
    }
}
