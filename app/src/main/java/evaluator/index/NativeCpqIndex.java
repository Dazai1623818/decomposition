package evaluator.index;

import dev.roanh.cpqindex.CanonForm;
import dev.roanh.cpqindex.CanonForm.CoreHash;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        this.stats = new EntryStats(index);
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
    public RelationSynopsis componentSynopsis(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        return stats.componentSynopsis(cpq);
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
        private final Map<CoreHash, RelationSynopsis> synopsisByCore;
        private final ConcurrentHashMap<String, CoreHash> coreHashByQuery = new ConcurrentHashMap<>();

        private EntryStats(Index index) {
            Objects.requireNonNull(index, "index");
            Map<CoreHash, RelationSynopsis> synopses = new HashMap<>();

            for (CoreHash core : index.getCoreHashes()) {
                synopses.put(core, aggregate(index.getBlocksForCoreHash(core)));
            }

            synopsisByCore = Map.copyOf(synopses);
        }

        private RelationSynopsis componentSynopsis(CPQ cpq) {
            return synopsisByCore.getOrDefault(coreHash(cpq), RelationSynopsis.empty());
        }

        private CoreHash coreHash(CPQ cpq) {
            return coreHashByQuery.computeIfAbsent(
                    cpq.toString(),
                    ignored -> CanonForm.computeCanon(cpq, false).toHashCanon());
        }

        private static RelationSynopsis aggregate(List<Index.Block> blocks) {
            Objects.requireNonNull(blocks, "blocks");
            if (blocks.isEmpty()) {
                return RelationSynopsis.empty();
            }

            int totalPairs = 0;
            for (Index.Block block : blocks) {
                totalPairs = Math.addExact(totalPairs, block.getPathCount());
            }

            int[] sources = new int[totalPairs];
            int[] targets = new int[totalPairs];
            int offset = 0;
            for (Index.Block block : blocks) {
                for (Pair pair : block.getPaths()) {
                    sources[offset] = pair.getSource();
                    targets[offset] = pair.getTarget();
                    offset++;
                }
            }
            int[] distinctSources = distinctValues(sources);
            int[] distinctTargets = distinctValues(targets);
            if (distinctSources.length == 0 || distinctTargets.length == 0) {
                return RelationSynopsis.empty();
            }

            long tupleCount = totalPairs;
            return new RelationSynopsis(
                    tupleCount,
                    endpointSummary(tupleCount, distinctSources.length),
                    endpointSummary(tupleCount, distinctTargets.length));
        }

        private static CpqIndex.EndpointSummary endpointSummary(long tupleCount, int distinctCount) {
            if (tupleCount <= 0L || distinctCount <= 0) {
                return CpqIndex.EndpointSummary.empty();
            }
            return new CpqIndex.EndpointSummary(distinctCount, (double) tupleCount / (double) distinctCount);
        }

        private static int[] distinctValues(int[] values) {
            if (values.length == 0) {
                return values;
            }
            Arrays.sort(values);
            int unique = 1;
            for (int i = 1; i < values.length; i++) {
                if (values[i] != values[unique - 1]) {
                    values[unique++] = values[i];
                }
            }
            return Arrays.copyOf(values, unique);
        }

    }
}
