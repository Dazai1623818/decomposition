package decomposition.cpq.model;

import decomposition.cpq.KnownComponent;
import java.util.List;
import java.util.Objects;

public record PartitionAnalysis(
    List<ComponentCPQExpressions> components,
    List<KnownComponent> preferredPerComponent,
    List<Integer> ruleCounts) {

  public PartitionAnalysis {
    components = List.copyOf(Objects.requireNonNull(components, "components"));
    preferredPerComponent =
        List.copyOf(Objects.requireNonNull(preferredPerComponent, "preferredPerComponent"));
    ruleCounts = List.copyOf(Objects.requireNonNull(ruleCounts, "ruleCounts"));
  }
}
