package dev.roanh.gmark.core.graph;

import dev.roanh.gmark.output.OutputXML;
import dev.roanh.gmark.util.IDable;
import dev.roanh.gmark.util.IndentWriter;
import java.util.Objects;

/**
 * Minimal implementation of the legacy {@code dev.roanh.gmark.core.graph.Predicate} expected by the
 * CPQ native index. It mirrors the historical behaviour sufficiently for the evaluation flow, while
 * remaining isolated from the modern gMark classes used by the decomposition pipeline.
 */
public final class Predicate implements OutputXML, Comparable<Predicate>, IDable {
  private final int id;
  private final String alias;
  private final Double proportion;
  private final boolean inverseFlag;
  private Predicate inverse;

  public Predicate(int id, String alias) {
    this(id, alias, null);
  }

  public Predicate(int id, String alias, Double proportion) {
    this(id, alias, proportion, false);
  }

  private Predicate(int id, String alias, Double proportion, boolean inverseFlag) {
    this.id = id;
    this.alias = alias;
    this.proportion = proportion;
    this.inverseFlag = inverseFlag;
  }

  /**
   * Returns {@code true} when this predicate corresponds to an inverse edge created via {@link
   * #getInverse()}.
   */
  public boolean isInverse() {
    return inverseFlag;
  }

  /** Alias is used as-is for forward edges and suffixed with {@code ⁻} for inverse edges. */
  public String getAlias() {
    return inverseFlag ? alias + "⁻" : alias;
  }

  public Predicate getInverse() {
    if (inverse == null) {
      inverse = new Predicate(id, alias, proportion, !inverseFlag);
      inverse.inverse = this;
    }
    return inverse;
  }

  @Override
  public int getID() {
    return id;
  }

  @Override
  public String toString() {
    return id + ":" + getAlias();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Predicate other)) {
      return false;
    }
    return id == other.id && inverseFlag == other.inverseFlag;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, inverseFlag);
  }

  @Override
  public void writeXML(IndentWriter writer) {
    writer.print(isInverse() ? "<symbol inverse=\"true\">" : "<symbol>");
    writer.print(id);
    writer.println("</symbol>");
  }

  @Override
  public int compareTo(Predicate other) {
    int comparison = Integer.compare(id, other.id);
    if (comparison != 0) {
      return comparison;
    }
    return Boolean.compare(inverseFlag, other.inverseFlag);
  }
}
