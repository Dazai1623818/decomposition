package decomposition.cpq;

import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.BitSet;

/**
 * Utility to build {@link KnownComponent} instances with defensive BitSet copies.
 */
final class KnownComponentFactory {

    private KnownComponentFactory() {
    }

    static KnownComponent create(CPQ cpq,
                                 BitSet edges,
                                 String source,
                                 String target,
                                 String derivation) {
        return new KnownComponent(cpq, BitsetUtils.copy(edges), source, target, derivation);
    }
}
