package decomposition.eval;

import dev.roanh.gmark.lang.cq.AtomCQ;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a tree decomposition in which each bag contains one or more atoms and bags are
 * organised hierarchically. The caller controls how the query is partitioned across bags.
 */
final class QueryDecomposition {
  private final Bag root;

  private QueryDecomposition(Bag root) {
    this.root = Objects.requireNonNull(root, "root");
  }

  static QueryDecomposition of(Bag root) {
    return new QueryDecomposition(root);
  }

  Bag root() {
    return root;
  }

  static Bag bag(List<AtomCQ> atoms) {
    return bag(atoms, List.of());
  }

  static Bag bag(List<AtomCQ> atoms, List<Bag> children) {
    return new Bag(atoms, children);
  }

  static final class Bag {
    private final List<AtomCQ> atoms;
    private final List<Bag> children;

    private Bag(List<AtomCQ> atoms, List<Bag> children) {
      Objects.requireNonNull(atoms, "atoms");
      if (atoms.isEmpty()) {
        throw new IllegalArgumentException("Decomposition bag must contain at least one atom.");
      }
      this.atoms = List.copyOf(atoms);
      if (children == null || children.isEmpty()) {
        this.children = List.of();
      } else {
        this.children = List.copyOf(new ArrayList<>(children));
      }
    }

    List<AtomCQ> atoms() {
      return atoms;
    }

    List<Bag> children() {
      return children;
    }
  }
}
