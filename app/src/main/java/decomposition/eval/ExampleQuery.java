package decomposition.eval;

import dev.roanh.gmark.lang.cq.CQ;
import java.util.Objects;
import java.util.Optional;

/**
 * Bundles an example CQ with an optional decomposition so callers can execute both the baseline
 * query and the decomposed variant.
 */
final class ExampleQuery {
  private final CQ cq;
  private final QueryDecomposition decomposition;

  private ExampleQuery(CQ cq, QueryDecomposition decomposition) {
    this.cq = Objects.requireNonNull(cq, "cq");
    this.decomposition = decomposition;
  }

  static ExampleQuery of(CQ cq) {
    return new ExampleQuery(cq, null);
  }

  static ExampleQuery of(CQ cq, QueryDecomposition decomposition) {
    return new ExampleQuery(cq, Objects.requireNonNull(decomposition, "decomposition"));
  }

  CQ cq() {
    return cq;
  }

  Optional<QueryDecomposition> decomposition() {
    return Optional.ofNullable(decomposition);
  }
}
