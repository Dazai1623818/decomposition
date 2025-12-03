package decomposition.eval.leapfrogjoiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Represents a constraint on one or two variables derived from a Component result. */
public final class RelationBinding {
  private final String sourceVar;
  private final String targetVar;
  private final String description;
  private final RelationProjection projection;
  private final int[] unaryDomain;

  private RelationBinding(
      String sourceVar, String targetVar, String description, RelationProjection projection) {
    this.sourceVar = Objects.requireNonNull(sourceVar, "sourceVar");
    this.targetVar = Objects.requireNonNull(targetVar, "targetVar");
    this.description = Objects.requireNonNull(description, "description");
    this.projection = Objects.requireNonNull(projection, "projection");
    this.unaryDomain = null;
  }

  private RelationBinding(String variable, String description, int[] unaryDomain) {
    this.sourceVar = Objects.requireNonNull(variable, "variable");
    this.targetVar = null;
    this.description = Objects.requireNonNull(description, "description");
    this.projection = null;
    this.unaryDomain = Objects.requireNonNull(unaryDomain, "unaryDomain");
  }

  public static RelationBinding binary(
      String sourceVar, String targetVar, String description, RelationProjection projection) {
    return new RelationBinding(sourceVar, targetVar, description, projection);
  }

  public static RelationBinding unary(String variable, String description, int[] domain) {
    return new RelationBinding(variable, description, domain);
  }

  public void register(Map<String, List<RelationBinding>> relationsPerVariable) {
    relationsPerVariable.computeIfAbsent(sourceVar, ignored -> new ArrayList<>()).add(this);
    if (targetVar != null) {
      relationsPerVariable.computeIfAbsent(targetVar, ignored -> new ArrayList<>()).add(this);
    }
  }

  public int[] domainFor(String variable, Map<String, Integer> assignment) {
    if (unaryDomain != null) {
      return unaryDomain;
    }
    if (variable.equals(sourceVar)) {
      if (assignment.containsKey(targetVar)) {
        int target = assignment.get(targetVar);
        return projection.sourcesForTarget(target);
      }
      return projection.allSources();
    }
    if (variable.equals(targetVar)) {
      if (assignment.containsKey(sourceVar)) {
        int source = assignment.get(sourceVar);
        return projection.targetsForSource(source);
      }
      return projection.allTargets();
    }
    throw new IllegalArgumentException(
        "Variable " + variable + " not part of relation " + description);
  }
}
