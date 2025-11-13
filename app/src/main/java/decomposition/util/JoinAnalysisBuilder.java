package decomposition.util;

import decomposition.model.Component;
import decomposition.model.Partition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class JoinAnalysisBuilder {

  private JoinAnalysisBuilder() {}

  public static JoinAnalysis analyzePartition(Partition partition, Set<String> freeVariables) {
    Map<String, Integer> multiplicity = new HashMap<>();

    for (Component component : partition.components()) {
      for (String vertex : component.vertices()) {
        multiplicity.merge(vertex, 1, Integer::sum);
      }
    }

    Set<String> globalJoinNodes = new HashSet<>();
    for (Map.Entry<String, Integer> entry : multiplicity.entrySet()) {
      String vertex = entry.getKey();
      if (entry.getValue() > 1 || freeVariables.contains(vertex)) {
        globalJoinNodes.add(vertex);
      }
    }

    globalJoinNodes.addAll(freeVariables);

    return new JoinAnalysis(
        Collections.unmodifiableMap(multiplicity), Collections.unmodifiableSet(globalJoinNodes));
  }
}
