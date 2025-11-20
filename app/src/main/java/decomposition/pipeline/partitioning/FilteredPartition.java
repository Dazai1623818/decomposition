package decomposition.pipeline.partitioning;

import decomposition.core.model.Component;
import decomposition.core.model.Partition;
import decomposition.util.JoinAnalysis;
import java.util.Map;
import java.util.Set;

public record FilteredPartition(Partition partition, JoinAnalysis joinAnalysis) {
  public Set<String> joinNodes() {
    return joinAnalysis.globalJoinNodes();
  }

  public Set<String> joinNodesForComponent(Component component) {
    return joinAnalysis.joinNodesForComponent(component);
  }

  public Map<String, Integer> multiplicity() {
    return joinAnalysis.multiplicity();
  }
}
