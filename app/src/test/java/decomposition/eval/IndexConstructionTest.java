package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Temporarily disabled; relies on native lib and external graph file")
final class IndexConstructionTest {

  @Test
  void buildsIndexForSmallGraph() throws Exception {
    System.load(Path.of("lib", "libnauty.so").toAbsolutePath().toString());

    Path graphPath = Path.of("graphs", "example1_micro.edge").toAbsolutePath();
    assertTrue(Files.exists(graphPath), "Test graph is missing: " + graphPath);

    UniqueGraph<Integer, Predicate> graph = IndexUtil.readGraph(graphPath);
    Index index =
        new Index(
            graph,
            3,
            true,
            true,
            Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
            2,
            ProgressListener.LOG);
    index.sort();

    // Verify index works by querying it
    assertNotNull(
        index.query(CPQ.label(new Predicate(0, "0"))), "Index query should return a result");
  }
}
