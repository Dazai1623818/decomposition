package decomposition;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class Example {

  public static CQ example1() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(b, new Predicate(2, "r2"), c);
    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), a);
    cq.addAtom(a, new Predicate(5, "r5"), c);

    return cq;
  }

  public static CQ example2() {
    CQ cq = CQ.empty();
    VarCQ b = cq.addFreeVariable("B");
    VarCQ a = cq.addBoundVariable("A");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(b, new Predicate(2, "r2"), c);
    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), a);
    cq.addAtom(a, new Predicate(5, "r5"), c);

    return cq;
  }

  public static CQ example3() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(b, new Predicate(2, "r2"), c);
    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), a);
    cq.addAtom(a, new Predicate(5, "r5"), c);
    cq.addAtom(a, new Predicate(6, "r1"), a);

    return cq;
  }

  public static CQ example4() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(b, new Predicate(2, "r2"), c);
    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), a);
    cq.addAtom(a, new Predicate(5, "r5"), c);
    cq.addAtom(a, new Predicate(6, "r1"), a);
    cq.addAtom(b, new Predicate(7, "r1"), d);

    return cq;
  }

  // short branch
  public static CQ example5() {
    CQ cq = CQ.empty();
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addFreeVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), e);
    cq.addAtom(d, new Predicate(5, "r5"), f);

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

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(b, new Predicate(1, "r1"), a);
    cq.addAtom(b, new Predicate(2, "r2"), c);
    cq.addAtom(c, new Predicate(3, "r3"), d);
    cq.addAtom(d, new Predicate(4, "r4"), e);
    cq.addAtom(d, new Predicate(5, "r5"), f);

    return cq;
  }

  // Example 7: Multiple edges between same nodes with different labels
  public static CQ example7() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(a, new Predicate(2, "r2"), b); // Multiple edge A->B
    cq.addAtom(b, new Predicate(3, "r3"), c);
    cq.addAtom(b, new Predicate(4, "r4"), c); // Multiple edge B->C
    cq.addAtom(c, new Predicate(5, "r5"), a);
    cq.addAtom(c, new Predicate(6, "r6"), a); // Multiple edge C->A
    cq.addAtom(c, new Predicate(7, "r6"), c); // Multiple edge C->A

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

    cq.addAtom(a, new Predicate(1, "r1"), b);
    cq.addAtom(a, new Predicate(2, "r2"), b); // Multiple edge A->B
    cq.addAtom(b, new Predicate(3, "r3"), c);
    cq.addAtom(c, new Predicate(4, "r4"), d);
    cq.addAtom(d, new Predicate(5, "r5"), e);
    cq.addAtom(e, new Predicate(6, "r6"), a);
    cq.addAtom(e, new Predicate(7, "r7"), a); // Multiple edge E->A
    cq.addAtom(c, new Predicate(8, "r8"), a);

    return cq;
  }

  public static CQ random(RandomExampleConfig config) {
    Objects.requireNonNull(config, "config");
    Random rng = config.createRandom();

    CQ cq = CQ.empty();
    List<VarCQ> variables = new ArrayList<>();

    for (int i = 0; i < config.freeVariableCount(); i++) {
      variables.add(cq.addFreeVariable("F" + i));
    }
    for (int i = 0; i < config.boundVariableCount(); i++) {
      variables.add(cq.addBoundVariable("B" + i));
    }

    if (variables.isEmpty()) {
      throw new IllegalStateException("Random example requires at least one variable");
    }

    for (int i = 0; i < config.edgeCount(); i++) {
      VarCQ source = variables.get(rng.nextInt(variables.size()));
      VarCQ target = variables.get(rng.nextInt(variables.size()));
      int predicateId = rng.nextInt(config.predicateLabelCount()) + 1;
      cq.addAtom(source, new Predicate(predicateId, "r" + predicateId), target);
    }

    return cq;
  }
}
