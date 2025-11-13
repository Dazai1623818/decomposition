package decomposition.util;

import decomposition.model.Component;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record JoinAnalysis(
    Map<String, Integer> multiplicity, // variable -> #components it appears in
    Set<String> globalJoinNodes // variables that are global join nodes
    ) {

  public Set<String> joinNodesForComponent(Component component) {
    return component.vertices().stream()
        .filter(globalJoinNodes::contains)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Map<String, Integer> multiplicity() {
    return Collections.unmodifiableMap(multiplicity);
  }

  @Override
  public Set<String> globalJoinNodes() {
    return Collections.unmodifiableSet(globalJoinNodes);
  }
}
