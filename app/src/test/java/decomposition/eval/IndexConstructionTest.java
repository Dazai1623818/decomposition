package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import decomposition.nativeindex.CpqNativeIndex;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class IndexConstructionTest {

  @Test
  void buildsIndexForSmallGraph() throws Exception {
    System.load(Path.of("lib", "libnauty.so").toAbsolutePath().toString());

    UniqueGraph<Integer, Predicate> graph = IndexUtil.readGraph(Path.of("graph.edge"));
    CpqNativeIndex index =
        new CpqNativeIndex(graph, 3, Math.max(1, Runtime.getRuntime().availableProcessors() - 1));
    index.sort();

    // Verify index works by querying it
    assertNotNull(
        index.query(CPQ.label(new Predicate(0, "0"))), "Index query should return a result");
  }
}
