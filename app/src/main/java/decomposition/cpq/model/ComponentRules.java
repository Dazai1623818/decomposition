package decomposition.cpq.model;

import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import java.util.List;
import java.util.Objects;

public record ComponentRules(
    Component component,
    List<KnownComponent> rawRules,
    List<KnownComponent> joinFilteredRules,
    List<KnownComponent> finalRules) {

  public ComponentRules {
    Objects.requireNonNull(component, "component");
    rawRules = List.copyOf(Objects.requireNonNull(rawRules, "rawRules"));
    joinFilteredRules = List.copyOf(Objects.requireNonNull(joinFilteredRules, "joinFilteredRules"));
    finalRules = List.copyOf(Objects.requireNonNull(finalRules, "finalRules"));
  }

  public KnownComponent preferred() {
    return finalRules.isEmpty() ? null : finalRules.get(0);
  }
}
