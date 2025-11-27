package decomposition.eval;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.ProgressListener;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the lifecycle of the native CPQ index. */
public final class IndexManager {
  private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);
  private static final AtomicBoolean NATIVES_LOADED = new AtomicBoolean();

  public Index loadOrBuild(Path graphPath, int k) throws IOException {
    ensureNativesLoaded();
    Path preferredIndexPath = resolveIndexPath(graphPath, k);
    Path existingIndexPath = findExistingIndexPath(graphPath, k);

    // 1. Try Load
    if (existingIndexPath != null) {
      if (!existingIndexPath.equals(preferredIndexPath)) {
        LOG.info("Loading existing index (fallback match): {}", existingIndexPath);
      } else {
        LOG.info("Loading existing index: {}", existingIndexPath);
      }
      Index index = load(existingIndexPath);
      index.sort();
      return index;
    }

    // 2. Build New
    LOG.info("Building new index for k={}: {}", k, graphPath);
    Index index;
    try {
      index =
          new Index(
              IndexUtil.readGraph(graphPath),
              k,
              true,
              true,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
              2,
              ProgressListener.LOG);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
    index.sort();

    // 3. Save (Diagnostics/Cache)
    try {
      save(index, preferredIndexPath);
      LOG.info("Index saved to {}", preferredIndexPath);
    } catch (IOException e) {
      LOG.warn("Failed to persist index (proceeding in-memory): {}", e.getMessage());
    }

    return index;
  }

  private Path resolveIndexPath(Path graphPath, int k) {
    String name = graphPath.getFileName().toString();
    String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
    return graphPath.resolveSibling(stem + ".k" + k + ".idx");
  }

  private Path findExistingIndexPath(Path graphPath, int k) {
    String name = graphPath.getFileName().toString();
    String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;

    // Prefer k-specific, but also allow precomputed generic .idx files that share the base name.
    Path kSpecific = resolveIndexPath(graphPath, k);
    Path stemOnly = graphPath.resolveSibling(stem + ".idx");
    Path fullName = graphPath.resolveSibling(name + ".idx");

    for (Path candidate : List.of(kSpecific, stemOnly, fullName)) {
      if (Files.exists(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  private Index load(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return new Index(in);
    }
  }

  private void ensureNativesLoaded() throws IOException {
    if (!NATIVES_LOADED.compareAndSet(false, true)) {
      return;
    }
    try {
      Main.loadNatives();
      LOG.info("Loaded native nauty bindings.");
    } catch (UnsatisfiedLinkError e) {
      NATIVES_LOADED.set(false);
      throw new IOException(
          "Failed to load native nauty bindings required by CPQ index: " + e.getMessage(), e);
    }
  }

  private void save(Index index, Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try (OutputStream out = Files.newOutputStream(path)) {
      index.write(out, false);
    } catch (RuntimeException ex) {
      // Some graphs may carry missing predicate aliases; avoid failing the main flow if persistence
      // is not possible.
      throw new IOException("Unable to persist native index: " + ex.getMessage(), ex);
    }
  }
}
