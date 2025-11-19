package decomposition.eval;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Evaluates bags via {@link LeapfrogCpqJoiner} and joins their assignments. */
final class JoinedDecompositionExecutor {
  private final LeapfrogCpqJoiner joiner;

  JoinedDecompositionExecutor(LeapfrogCpqJoiner joiner) {
    this.joiner = Objects.requireNonNull(joiner, "joiner");
  }

  List<Map<String, Integer>> execute(QueryDecomposition decomposition) {
    Objects.requireNonNull(decomposition, "decomposition");
    return evaluate(decomposition.root());
  }

  private List<Map<String, Integer>> evaluate(QueryDecomposition.Bag bag) {
    List<Map<String, Integer>> bagResults = joiner.executeOptimized(bag.atoms());
    for (QueryDecomposition.Bag child : bag.children()) {
      List<Map<String, Integer>> childResults = evaluate(child);
      bagResults = naturalJoin(bagResults, childResults);
      if (bagResults.isEmpty()) {
        break;
      }
    }
    return bagResults;
  }

  private static List<Map<String, Integer>> naturalJoin(
      List<Map<String, Integer>> left, List<Map<String, Integer>> right) {
    if (left.isEmpty() || right.isEmpty()) {
      return List.of();
    }
    List<Map<String, Integer>> result = new ArrayList<>();
    for (Map<String, Integer> leftRow : left) {
      for (Map<String, Integer> rightRow : right) {
        if (!compatible(leftRow, rightRow)) {
          continue;
        }
        Map<String, Integer> joined = new LinkedHashMap<>(leftRow);
        for (Map.Entry<String, Integer> entry : rightRow.entrySet()) {
          joined.put(entry.getKey(), entry.getValue());
        }
        result.add(joined);
      }
    }
    return result;
  }

  private static boolean compatible(Map<String, Integer> left, Map<String, Integer> right) {
    for (Map.Entry<String, Integer> entry : left.entrySet()) {
      Integer other = right.get(entry.getKey());
      if (other != null && !Objects.equals(entry.getValue(), other)) {
        return false;
      }
    }
    return true;
  }
}
