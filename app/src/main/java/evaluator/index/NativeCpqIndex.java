package evaluator.index;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class NativeCpqIndex implements CpqIndex {
    static {
        try {
            Main.loadNatives();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Index index;
    private final EntryStats stats;
    private final int k;
    private final int maxIntersections;

    private NativeCpqIndex(Index index, int k, int maxIntersections) {
        this.index = Objects.requireNonNull(index, "index");
        this.stats = new EntryStats();
        this.k = k;
        this.maxIntersections = maxIntersections;
    }

    /**
     * Loads one CPQ-native index file.
     * Directory-based subset-index loading is intentionally unsupported.
     * @param savedIndexFile Path to one serialized index file.
     * @return Loaded CPQ-native index wrapper.
     * @throws Exception When loading fails.
     */
    public static NativeCpqIndex load(Path savedIndexFile) throws Exception {
        Objects.requireNonNull(savedIndexFile, "savedIndexFile");
        if (Files.isDirectory(savedIndexFile)) {
            throw new IllegalArgumentException("Index directories are unsupported: " + savedIndexFile);
        }
        if (!Files.exists(savedIndexFile) || !Files.isRegularFile(savedIndexFile)) {
            throw new IllegalArgumentException("Index file not found: " + savedIndexFile);
        }
        try (InputStream in = Files.newInputStream(savedIndexFile)) {
            Index index = new Index(in);
            return new NativeCpqIndex(index, index.getK(), index.getIntersections());
        }
    }

    @Override
    public int k() {
        return k;
    }

    @Override
    public int intersections() {
        return maxIntersections;
    }

    @Override
    public List<Edge> query(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        List<Pair> pairs = index.query(cpq);
        List<Edge> edges = new ArrayList<>(pairs.size());
        for (Pair pair : pairs) {
            edges.add(new Edge(pair.getSource(), pair.getTarget()));
        }
        return edges;
    }

    @Override
    public long cost(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return index.cost(cpq);
    }

    @Override
    public ComponentStats componentStats(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return stats.componentStats(index, cpq);
    }

    @Override
    public RelationSynopsis componentSynopsis(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return stats.componentSynopsis(index, cpq);
    }

    /**
     * Checks whether the loaded index can answer the given CPQ based on its diameter.
     * @param cpq The CPQ to check.
     * @return True if the loaded index can answer the CPQ.
     */
    @Override
    public boolean isIndexable(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return cpq.getDiameter() <= k;
    }

    private static final class EntryStats {
        private final ConcurrentHashMap<String, ComponentStats> componentStatsByQuery = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, RelationSynopsis> componentSynopsisByQuery = new ConcurrentHashMap<>();

        private ComponentStats componentStats(Index index, CPQ cpq) {
            return componentStatsByQuery.computeIfAbsent(cpq.toString(), ignored -> aggregate(index.query(cpq)));
        }

        private RelationSynopsis componentSynopsis(Index index, CPQ cpq) {
            return componentSynopsisByQuery.computeIfAbsent(
                    cpq.toString(),
                    ignored -> componentStats(index, cpq).toSynopsis());
        }

        private ComponentStats aggregate(List<Pair> pairs) {
            if (pairs.isEmpty()) {
                return ComponentStats.empty();
            }
            int[] sources = new int[pairs.size()];
            int[] targets = new int[pairs.size()];
            for (int i = 0; i < pairs.size(); i++) {
                Pair pair = pairs.get(i);
                sources[i] = pair.getSource();
                targets[i] = pair.getTarget();
            }
            return new ComponentStats(pairs.size(), sources, targets);
        }
    }
}
