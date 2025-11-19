package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.core.graph.Predicate;
import dev.roanh.gmark.util.UniqueGraph;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class IndexConstructionTest {

  @Test
  void buildsIndexForSmallGraph() throws Exception {
    System.load(Path.of("lib", "libnauty.so").toAbsolutePath().toString());

    UniqueGraph<Integer, Predicate> graph = GraphLoader.load(Path.of("graph.edge"));
    Index index =
        new Index(
            graph,
            3,
            false,
            true,
            Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
            -1,
            ProgressListener.NONE);
    index.sort();

    assertFalse(index.getBlocks().isEmpty(), "Index should contain at least one block");
  }
}
