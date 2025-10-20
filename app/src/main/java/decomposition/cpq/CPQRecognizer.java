package decomposition.cpq;

import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Recognizes CPQ expressions for connected components of the IR.
 */
public final class CPQRecognizer {
    private final ComponentCPQBuilder builder;
    private final Map<String, Optional<KnownComponent>> memo = new HashMap<>();
    private final int edgeCount;

    public CPQRecognizer(List<Edge> edges) {
        this.builder = new ComponentCPQBuilder(edges);
        this.edgeCount = builder.totalEdges();
    }

    public Optional<KnownComponent> recognize(Component component) {
        BitSet edgeBits = component.edgeBits();
        String signature = BitsetUtils.signature(edgeBits, edgeCount);
        return memo.computeIfAbsent(signature, key -> {
            List<KnownComponent> options = builder.options(edgeBits);
            return options.isEmpty() ? Optional.empty() : Optional.of(options.get(0));
        });
    }

    public List<KnownComponent> enumerateAll(Component component) {
        return builder.options(component.edgeBits());
    }
}
