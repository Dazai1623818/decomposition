package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Partition;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates partitions against the CPQ builder and enumerates component combinations.
 */
public final class PartitionValidator {

    public boolean isValidCPQDecomposition(Partition partition, ComponentCPQBuilder builder) {
        for (Component component : partition.components()) {
            if (builder.options(component.edgeBits()).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public List<List<KnownComponent>> enumerateDecompositions(
            Partition partition,
            ComponentCPQBuilder builder,
            int limit) {
        List<List<KnownComponent>> perComponentOptions = new ArrayList<>();
        for (Component component : partition.components()) {
            List<KnownComponent> options = builder.options(component.edgeBits());
            if (options.isEmpty()) {
                return List.of();
            }
            perComponentOptions.add(options);
        }
        return cartesian(perComponentOptions, limit);
    }

    private List<List<KnownComponent>> cartesian(List<List<KnownComponent>> lists, int limit) {
        List<List<KnownComponent>> output = new ArrayList<>();
        backtrack(lists, 0, new ArrayList<>(), output, limit);
        return output;
    }

    private void backtrack(List<List<KnownComponent>> lists,
                           int index,
                           List<KnownComponent> current,
                           List<List<KnownComponent>> output,
                           int limit) {
        if (limit > 0 && output.size() >= limit) {
            return;
        }
        if (index == lists.size()) {
            output.add(List.copyOf(current));
            return;
        }
        for (KnownComponent option : lists.get(index)) {
            current.add(option);
            backtrack(lists, index + 1, current, output, limit);
            current.remove(current.size() - 1);
            if (limit > 0 && output.size() >= limit) {
                return;
            }
        }
    }
}
