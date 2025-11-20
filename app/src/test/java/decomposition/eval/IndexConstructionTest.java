package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertFalse;

import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.ProgressListener;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class IndexConstructionTest {

  @Test
  void buildsIndexForSmallGraph() throws Exception {
    System.load(Path.of("lib", "libnauty.so").toAbsolutePath().toString());

    UniqueGraph<Integer, Predicate> graph = GraphLoader.load(Path.of("graph.edge"));
    CpqNativeIndex index =
        new CpqNativeIndex(
            graph,
            3,
            false,
            true,
            Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
            -1,
            ProgressListener.none());
    index.sort();

    assertFalse(index.getBlocks().isEmpty(), "Index should contain at least one block");
  }
}
