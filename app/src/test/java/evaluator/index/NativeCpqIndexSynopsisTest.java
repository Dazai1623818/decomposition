package evaluator.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.index.CpqIndex.ComponentStats;
import evaluator.index.CpqIndex.Edge;
import evaluator.index.CpqIndex.Endpoint;
import evaluator.index.CpqIndex.RelationSynopsis;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeCpqIndexSynopsisTest {
    private static final Path INDEX_PATH = Path.of("local/indices/robotssmall.k2.idx");
    private static final int LABEL_SEARCH_LIMIT = 50;

    @TempDir
    Path tempDir;

    @Test
    void defaultComponentSynopsisFallsBackToComponentStats() {
        AggregateOnlyIndex index = new AggregateOnlyIndex();
        CPQ cpq = CPQ.label(new Predicate(0, "0"));

        ComponentStats stats = index.componentStats(cpq);
        RelationSynopsis synopsis = index.componentSynopsis(cpq);

        assertEquals(stats.tupleCount(), synopsis.tupleCount());
        assertEquals(stats.distinctValues(Endpoint.SOURCE), synopsis.distinctValues(Endpoint.SOURCE));
        assertEquals(stats.distinctValues(Endpoint.TARGET), synopsis.distinctValues(Endpoint.TARGET));
    }

    @Test
    void nativeComponentSynopsisMatchesComponentStats() throws Exception {
        Assumptions.assumeTrue(Files.exists(INDEX_PATH), "Missing index file " + INDEX_PATH);
        NativeCpqIndex index = NativeCpqIndex.load(INDEX_PATH);
        LabelQuery labelQuery = findLabelWithPairs(index);
        Assumptions.assumeTrue(labelQuery != null, "No label produced matches in index");

        CPQ cpq = CPQ.label(labelQuery.predicate());
        ComponentStats stats = index.componentStats(cpq);
        RelationSynopsis synopsis = index.componentSynopsis(cpq);

        assertFalse(stats.isEmpty());
        assertEquals(stats.tupleCount(), synopsis.tupleCount());
        assertEquals(stats.distinctValues(Endpoint.SOURCE), synopsis.distinctValues(Endpoint.SOURCE));
        assertEquals(stats.distinctValues(Endpoint.TARGET), synopsis.distinctValues(Endpoint.TARGET));
    }

    @Test
    void loadRejectsIndexDirectories() {
        assertThrows(IllegalArgumentException.class, () -> NativeCpqIndex.load(tempDir));
    }

    private static LabelQuery findLabelWithPairs(NativeCpqIndex index) {
        for (int id = 0; id <= LABEL_SEARCH_LIMIT; id++) {
            Predicate predicate = new Predicate(id, String.valueOf(id));
            if (!index.query(CPQ.label(predicate)).isEmpty()) {
                return new LabelQuery(predicate);
            }
        }
        return null;
    }

    private record LabelQuery(Predicate predicate) {
    }

    private static final class AggregateOnlyIndex implements CpqIndex {
        @Override
        public int k() {
            return 2;
        }

        @Override
        public int intersections() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isIndexable(CPQ cpq) {
            return true;
        }

        @Override
        public long cost(CPQ cpq) {
            return query(cpq).size();
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return List.of(
                    new Edge(1, 10),
                    new Edge(1, 11),
                    new Edge(2, 11));
        }
    }
}
