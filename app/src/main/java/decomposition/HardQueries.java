package decomposition;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

public final class HardQueries {

  private HardQueries() {}

  // Wide star to maximize fan-out at the free variable.
  public static CQ starOut5() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(a, label(3), d);
    cq.addAtom(a, label(4), e);
    cq.addAtom(a, label(5), f);

    return cq;
  }

  // Diamond into D with branching out of D.
  public static CQ diamondBranch() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(c, label(4), d);
    cq.addAtom(d, label(5), e);
    cq.addAtom(d, label(6), f);

    return cq;
  }

  // Long path with a branch at the middle.
  public static CQ pathBranchMid() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(c, label(4), e);
    cq.addAtom(c, label(5), f);

    return cq;
  }

  // Two stars connected through a path, with branching at the second hub.
  public static CQ twoStarsBridge() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");
    VarCQ g = cq.addBoundVariable("G");
    VarCQ h = cq.addBoundVariable("H");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(d, label(4), e);
    cq.addAtom(d, label(5), f);
    cq.addAtom(e, label(6), g);
    cq.addAtom(f, label(7), h);

    return cq;
  }

  // Cycle with two branches.
  public static CQ cycleWithBranches() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), e);
    cq.addAtom(c, label(6), f);

    return cq;
  }

  // Multiple edges between the same endpoints plus a tail.
  public static CQ multiEdgeHub() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), b);
    cq.addAtom(a, label(3), b);
    cq.addAtom(b, label(4), c);
    cq.addAtom(b, label(5), d);

    return cq;
  }

  private static Predicate label(int id) {
    return new Predicate(id, String.valueOf(id));
  }
}
