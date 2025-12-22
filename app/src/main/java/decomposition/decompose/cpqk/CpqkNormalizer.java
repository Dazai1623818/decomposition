package decomposition.decompose.cpqk;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.QueryLanguageSyntax;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class CpqkNormalizer {
  record Normalized(CPQ cpq, String canonical) {}

  private CpqkNormalizer() {}

  static Normalized normalize(CPQ cpq) {
    Objects.requireNonNull(cpq, "cpq");
    return normalizeTree(cpq.toAbstractSyntaxTree());
  }

  private static Normalized normalizeTree(QueryTree node) {
    return switch (node.getOperation()) {
      case IDENTITY -> new Normalized(CPQ.id(), "id");
      case EDGE -> normalizeEdge(node);
      case CONCATENATION -> normalizeConcatenation(node);
      case INTERSECTION -> normalizeIntersection(node);
      default ->
          throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
    };
  }

  private static Normalized normalizeEdge(QueryTree node) {
    Predicate label = node.getEdgeAtom().getLabel();
    return new Normalized(CPQ.label(label), label.getAlias());
  }

  private static void collectConcatenation(QueryTree node, List<Normalized> out) {
    if (node.getOperation() == OperationType.CONCATENATION) {
      collectConcatenation(node.getOperand(0), out);
      collectConcatenation(node.getOperand(1), out);
      return;
    }
    out.add(normalizeTree(node));
  }

  private static Normalized normalizeConcatenation(QueryTree node) {
    List<Normalized> parts = new ArrayList<>();
    collectConcatenation(node, parts);

    parts.removeIf(part -> part.cpq().getOperationType() == OperationType.IDENTITY);
    if (parts.isEmpty()) {
      return new Normalized(CPQ.id(), "id");
    }
    if (parts.size() == 1) {
      return parts.get(0);
    }

    List<CPQ> cpqs = new ArrayList<>(parts.size());
    StringBuilder canonical = new StringBuilder();
    canonical.append("(");
    for (int i = 0; i < parts.size(); i++) {
      cpqs.add(parts.get(i).cpq());
      if (i > 0) {
        canonical.append(QueryLanguageSyntax.CHAR_JOIN);
      }
      canonical.append(parts.get(i).canonical());
    }
    canonical.append(")");

    return new Normalized(CPQ.concat(cpqs), canonical.toString());
  }

  private static void collectIntersection(QueryTree node, List<Normalized> out) {
    if (node.getOperation() == OperationType.INTERSECTION) {
      collectIntersection(node.getOperand(0), out);
      collectIntersection(node.getOperand(1), out);
      return;
    }
    out.add(normalizeTree(node));
  }

  private static Normalized normalizeIntersection(QueryTree node) {
    List<Normalized> parts = new ArrayList<>();
    collectIntersection(node, parts);

    Map<String, Normalized> unique = new HashMap<>();
    for (Normalized part : parts) {
      unique.putIfAbsent(part.canonical(), part);
    }

    List<Normalized> ordered = new ArrayList<>(unique.values());
    ordered.sort(Comparator.comparing(Normalized::canonical));
    if (ordered.size() == 1) {
      return ordered.get(0);
    }

    List<CPQ> cpqs = new ArrayList<>(ordered.size());
    StringBuilder canonical = new StringBuilder();
    canonical.append("(");
    for (int i = 0; i < ordered.size(); i++) {
      cpqs.add(ordered.get(i).cpq());
      if (i > 0) {
        canonical.append(QueryLanguageSyntax.CHAR_INTERSECTION);
      }
      canonical.append(ordered.get(i).canonical());
    }
    canonical.append(")");

    return new Normalized(CPQ.intersect(cpqs), canonical.toString());
  }
}
