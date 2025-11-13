package decomposition.cpq;

import java.util.Set;

record RuleCacheKey(String signature, Set<String> joinNodes, int edgeCount) {
  RuleCacheKey {
    joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
  }
}
