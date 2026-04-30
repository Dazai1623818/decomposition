package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MechanismProbeMainTest {
    private static final Path INDEX_PATH = Path.of("index/robots.k2.idx");
    private static final String QUERY = "(x,y) ← 0(x,y)";

    @Test
    void standaloneProbeWritesIntermediateStages(@TempDir Path tempDir) throws Exception {
        Assumptions.assumeTrue(Files.exists(INDEX_PATH), "Missing index file " + INDEX_PATH);

        Path casesFile = tempDir.resolve("cases.tsv");
        Files.writeString(
                casesFile,
                String.join(
                        "\n",
                        "case_id\tcase_label\tdataset\ttemplate\tmethod\tindex_path\tquery_text\tvariable_order",
                        "case-1\tsmoke\trobots\tedge\tsingle_edge\t"
                                + INDEX_PATH
                                + "\t"
                                + QUERY
                                + "\t[?x,?y]",
                        ""),
                StandardCharsets.UTF_8);

        MechanismProbeMain.main(new String[] {
                "--cases-file", casesFile.toString(),
                "--output-dir", tempDir.toString(),
                "--cover-limit", "10",
                "--k", "2",
                "--decomposition-timeout-ms", "10000",
                "--execution-timeout-ms", "10000"
        });

        Path summaryPath = tempDir.resolve("probe_summary.tsv");
        Path stepsPath = tempDir.resolve("probe_intermediate_steps.tsv");
        assertTrue(Files.exists(summaryPath));
        assertTrue(Files.exists(stepsPath));

        String summary = Files.readString(summaryPath, StandardCharsets.UTF_8);
        String steps = Files.readString(stepsPath, StandardCharsets.UTF_8);
        assertTrue(summary.contains("\tOK\t"));
        assertTrue(steps.contains("\tmat\t"));
        assertTrue(steps.contains("\tans\t"));
    }
}
