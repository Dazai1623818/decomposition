package decomposition.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Collection of components that forms a disjoint cover of the CQ edges. */
public record Partition(List<Component> components) {

  public Partition {
    Objects.requireNonNull(components, "components");
    components = List.copyOf(components);
  }

  public int size() {
    return components.size();
  }

  public Component component(int index) {
    return components.get(index);
  }

  public List<Component> components() {
    return Collections.unmodifiableList(components);
  }
}
