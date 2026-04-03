package evaluator.bench;

import evaluator.index.CpqIndex;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Process-level memory diagnostics used by benchmark logging.
 *
 * <p>Heap and non-heap usage come from the JVM management beans. RSS and RSS
 * high-water mark are read from {@code /proc/self/status} when available.
 */
public final class MemoryDiagnostics {
    private static final long UNAVAILABLE = -1L;
    private static final Path PROC_SELF_STATUS = Path.of("/proc/self/status");

    private MemoryDiagnostics() {
    }

    public static ProcessSnapshot captureSnapshot() {
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = bean.getHeapMemoryUsage();
        MemoryUsage nonHeap = bean.getNonHeapMemoryUsage();
        ProcStatusMemory proc = readProcStatusMemory();
        return new ProcessSnapshot(
                heap.getUsed(),
                heap.getCommitted(),
                heap.getMax(),
                nonHeap.getUsed(),
                nonHeap.getCommitted(),
                nonHeap.getMax(),
                proc.rssBytes(),
                proc.rssHighWaterMarkBytes());
    }

    public static SectionToken beginSection() {
        resetPeakUsage();
        return new SectionToken(captureSnapshot());
    }

    public static SectionUsage endSection(SectionToken token) {
        PeakSnapshot peak = capturePeakUsage();
        return new SectionUsage(token.before(), captureSnapshot(), peak.heapPeakBytes(), peak.nonHeapPeakBytes());
    }

    public static IndexLoadStats completeIndexLoad(
            Path indexPath,
            CpqIndex index,
            long loadNanos,
            ProcessSnapshot beforeLoad,
            ProcessSnapshot afterLoad) {
        long fileBytes = safeFileSize(indexPath);
        return new IndexLoadStats(
                indexPath == null ? "-" : indexPath.toAbsolutePath().toString(),
                fileBytes,
                index.k(),
                index.intersections(),
                loadNanos,
                beforeLoad,
                afterLoad);
    }

    public static IndexLoadStats unavailable(String indexPath, CpqIndex index) {
        ProcessSnapshot unavailable = ProcessSnapshot.unavailable();
        return new IndexLoadStats(
                indexPath == null ? "-" : indexPath,
                UNAVAILABLE,
                index.k(),
                index.intersections(),
                UNAVAILABLE,
                unavailable,
                unavailable);
    }

    static String formatBytes(long bytes) {
        return Long.toString(bytes);
    }

    static long deltaBytes(long before, long after) {
        if (before < 0L || after < 0L) {
            return UNAVAILABLE;
        }
        return after - before;
    }

    private static long safeFileSize(Path path) {
        if (path == null) {
            return UNAVAILABLE;
        }
        try {
            return Files.size(path);
        } catch (IOException ex) {
            return UNAVAILABLE;
        }
    }

    private static void resetPeakUsage() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            try {
                pool.resetPeakUsage();
            } catch (RuntimeException ignored) {
                // Peak reset support is implementation-dependent.
            }
        }
    }

    private static PeakSnapshot capturePeakUsage() {
        long heapPeakBytes = 0L;
        long nonHeapPeakBytes = 0L;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            MemoryUsage peakUsage = pool.getPeakUsage();
            if (peakUsage == null || peakUsage.getUsed() < 0L) {
                continue;
            }
            if (pool.getType() == MemoryType.HEAP) {
                heapPeakBytes += peakUsage.getUsed();
            } else {
                nonHeapPeakBytes += peakUsage.getUsed();
            }
        }
        return new PeakSnapshot(heapPeakBytes, nonHeapPeakBytes);
    }

    private static ProcStatusMemory readProcStatusMemory() {
        if (!Files.isReadable(PROC_SELF_STATUS)) {
            return ProcStatusMemory.unavailable();
        }
        long rssBytes = UNAVAILABLE;
        long rssHighWaterMarkBytes = UNAVAILABLE;
        try (BufferedReader reader = Files.newBufferedReader(PROC_SELF_STATUS)) {
            for (String line; (line = reader.readLine()) != null;) {
                if (line.startsWith("VmRSS:")) {
                    rssBytes = parseStatusKilobytes(line);
                } else if (line.startsWith("VmHWM:")) {
                    rssHighWaterMarkBytes = parseStatusKilobytes(line);
                }
            }
        } catch (IOException ignored) {
            return ProcStatusMemory.unavailable();
        }
        return new ProcStatusMemory(rssBytes, rssHighWaterMarkBytes);
    }

    private static long parseStatusKilobytes(String line) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length < 2) {
            return UNAVAILABLE;
        }
        try {
            return Long.parseLong(parts[1]) * 1024L;
        } catch (NumberFormatException ex) {
            return UNAVAILABLE;
        }
    }

    public record ProcessSnapshot(
            long heapUsedBytes,
            long heapCommittedBytes,
            long heapMaxBytes,
            long nonHeapUsedBytes,
            long nonHeapCommittedBytes,
            long nonHeapMaxBytes,
            long rssBytes,
            long rssHighWaterMarkBytes) {
        public static ProcessSnapshot unavailable() {
            return new ProcessSnapshot(
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE,
                    UNAVAILABLE);
        }
    }

    public record SectionToken(ProcessSnapshot before) {
    }

    public record SectionUsage(
            ProcessSnapshot before,
            ProcessSnapshot after,
            long heapPeakBytes,
            long nonHeapPeakBytes) {
        public static SectionUsage unavailable() {
            ProcessSnapshot unavailable = ProcessSnapshot.unavailable();
            return new SectionUsage(unavailable, unavailable, UNAVAILABLE, UNAVAILABLE);
        }
    }

    public record IndexLoadStats(
            String indexPath,
            long indexFileBytes,
            int indexK,
            int indexIntersections,
            long loadNanos,
            ProcessSnapshot beforeLoad,
            ProcessSnapshot afterLoad) {
    }

    private record PeakSnapshot(long heapPeakBytes, long nonHeapPeakBytes) {
    }

    private record ProcStatusMemory(long rssBytes, long rssHighWaterMarkBytes) {
        private static ProcStatusMemory unavailable() {
            return new ProcStatusMemory(UNAVAILABLE, UNAVAILABLE);
        }
    }
}
