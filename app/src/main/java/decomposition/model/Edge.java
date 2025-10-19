package decomposition.model;

import dev.roanh.gmark.type.schema.Predicate;
import java.util.Objects;

/**
 * Immutable representation of a CQ edge (binary atom) in the intermediate model.
 */
public final class Edge {
    private final String source;
    private final String target;
    private final Predicate predicate;
    private final long syntheticId;

    public Edge(String source, String target, Predicate predicate, long syntheticId) {
        this.source = Objects.requireNonNull(source, "source");
        this.target = Objects.requireNonNull(target, "target");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
        this.syntheticId = syntheticId;
    }

    public String source() {
        return source;
    }

    public String target() {
        return target;
    }

    public Predicate predicate() {
        return predicate;
    }

    /**
     * Optional unique identifier, useful if multiple identical edges can appear.
     */
    public long syntheticId() {
        return syntheticId;
    }

    public String label() {
        return predicate.getAlias();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Edge other)) {
            return false;
        }
        return syntheticId == other.syntheticId
                && source.equals(other.source)
                && target.equals(other.target)
                && predicate.equals(other.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, predicate, syntheticId);
    }

    @Override
    public String toString() {
        return source + " -" + predicate.getAlias() + "-> " + target;
    }
}
