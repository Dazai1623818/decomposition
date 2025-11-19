package decomposition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

public class Example {

  public static CQ example1() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), c);

    return cq;
  }

  public static CQ example2() {
    CQ cq = CQ.empty();
    VarCQ b = cq.addFreeVariable("B");
    VarCQ a = cq.addBoundVariable("A");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), c);

    return cq;
  }

  public static CQ example3() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), c);
    cq.addAtom(a, label(6), a);

    return cq;
  }

  public static CQ example4() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), c);
    cq.addAtom(a, label(6), a);
    cq.addAtom(b, label(7), d);

    return cq;
  }

  // short branch
  public static CQ example5() {
    CQ cq = CQ.empty();
    VarCQ c = cq.addFreeVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), e);
    cq.addAtom(d, label(5), f);
    cq.addAtom(e, label(6), f);

    return cq;
  }

  // longer branch
  public static CQ example6() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(1), a);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), e);
    cq.addAtom(d, label(5), f);

    return cq;
  }

  // Example 7: Multiple edges between same nodes with different labels
  public static CQ example7() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), b); // Multiple edge A->B
    cq.addAtom(b, label(3), c);
    cq.addAtom(b, label(4), c); // Multiple edge B->C
    cq.addAtom(c, label(5), a);
    cq.addAtom(c, label(6), a); // Multiple edge C->A
    cq.addAtom(c, label(7), c); // Multiple edge C->A

    return cq;
  }

  // Example 8: Combination of long path and multiple edges
  public static CQ example8() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), b); // Multiple edge A->B
    cq.addAtom(b, label(3), c);
    cq.addAtom(c, label(4), d);
    cq.addAtom(d, label(5), e);
    cq.addAtom(e, label(6), a);
    cq.addAtom(e, label(7), a); // Multiple edge E->A
    cq.addAtom(c, label(8), a);

    return cq;
  }

  private static Predicate label(int id) {
    return new Predicate(id, String.valueOf(id));
  }

  public static CQ random(RandomExampleConfig config) {
    Objects.requireNonNull(config, "config");
    Random rng = config.createRandom();

    CQ cq = CQ.empty();
    List<VarCQ> freeVars = new ArrayList<>();

    for (int i = 0; i < config.freeVariableCount(); i++) {
      freeVars.add(cq.addFreeVariable("F" + i));
    }

    if (freeVars.isEmpty()) {
      throw new IllegalStateException("Random example requires at least one variable");
    }

    int minEdgesForConnectivity = freeVars.size() > 1 ? freeVars.size() - 1 : 0;
    if (config.edgeCount() < minEdgesForConnectivity) {
      throw new IllegalArgumentException(
          "edgeCount must be at least "
              + minEdgesForConnectivity
              + " to connect all free variables");
    }

    List<VarCQ> connected = new ArrayList<>();
    List<VarCQ> unconnectedFree = new ArrayList<>(freeVars);
    if (!unconnectedFree.isEmpty()) {
      connected.add(removeRandom(unconnectedFree, rng));
    }
    int boundCounter = 0;

    int edgesAdded = 0;
    while (edgesAdded < config.edgeCount()) {
      VarCQ anchor = connected.get(rng.nextInt(connected.size()));

      int edgesRemaining = config.edgeCount() - edgesAdded;
      boolean mustAttachFree =
          !unconnectedFree.isEmpty() && edgesRemaining <= unconnectedFree.size();

      VarCQ other;
      if (mustAttachFree) {
        other = removeRandom(unconnectedFree, rng);
        connected.add(other);
      } else {
        boolean canAttachExisting = connected.size() > 1;
        boolean canAttachFree = !unconnectedFree.isEmpty();

        int optionCount = 1; // self-loop
        if (canAttachExisting) optionCount++;
        if (canAttachFree) optionCount++;
        optionCount++; // always allow new bound variable

        int choice = rng.nextInt(optionCount);
        int index = 0;
        if (choice == index++) {
          other = anchor;
        } else if (canAttachExisting && choice == index++) {
          other = pickExistingOther(connected, rng, anchor);
        } else if (canAttachFree && choice == index++) {
          other = removeRandom(unconnectedFree, rng);
          connected.add(other);
        } else {
          VarCQ bound = cq.addBoundVariable("B" + boundCounter++);
          connected.add(bound);
          other = bound;
        }
      }

      addRandomlyOrientedEdge(cq, rng, anchor, other, config);
      edgesAdded++;
    }

    return cq;
  }

  private static void addRandomlyOrientedEdge(
      CQ cq, Random rng, VarCQ a, VarCQ b, RandomExampleConfig config) {
    VarCQ source = a;
    VarCQ target = b;
    if (!source.equals(target) && rng.nextBoolean()) {
      source = b;
      target = a;
    }
    int predicateId = rng.nextInt(config.predicateLabelCount()) + 1;
    cq.addAtom(source, label(predicateId), target);
  }

  private static VarCQ removeRandom(List<VarCQ> variables, Random rng) {
    return variables.remove(rng.nextInt(variables.size()));
  }

  private static VarCQ pickExistingOther(List<VarCQ> connected, Random rng, VarCQ anchor) {
    if (connected.size() <= 1) {
      return anchor;
    }
    while (true) {
      VarCQ candidate = connected.get(rng.nextInt(connected.size()));
      if (!candidate.equals(anchor)) {
        return candidate;
      }
    }
  }
}
