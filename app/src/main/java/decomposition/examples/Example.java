package decomposition.examples;

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
    // VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    // cq.addAtom(c, label(3), d);
    // cq.addAtom(d, label(4), a);
    // cq.addAtom(a, label(3), c);

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

  // Random query generation was removed along with RandomExampleConfig.
}
