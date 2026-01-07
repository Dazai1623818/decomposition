package decomposition;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

public final class SimpleQueries {

  private SimpleQueries() {}

  public static CQ starOut3() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(a, label(3), d);

    return cq;
  }

  public static CQ starIn3() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(b, label(1), a);
    cq.addAtom(c, label(2), a);
    cq.addAtom(d, label(3), a);

    return cq;
  }

  public static CQ starMixed4() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");

    cq.addAtom(a, label(1), b);
    cq.addAtom(c, label(2), a);
    cq.addAtom(a, label(3), d);
    cq.addAtom(e, label(4), a);

    return cq;
  }

  public static CQ starTwoLayer() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(c, label(4), e);

    return cq;
  }

  public static CQ path4() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);

    return cq;
  }

  public static CQ cycle4() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);

    return cq;
  }

  public static CQ diamondTwoFree() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ d = cq.addFreeVariable("D");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(c, label(4), d);

    return cq;
  }

  public static CQ triangle() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), a);

    return cq;
  }

  public static CQ bowtie() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), a);
    cq.addAtom(a, label(4), d);
    cq.addAtom(d, label(5), e);
    cq.addAtom(e, label(6), a);

    return cq;
  }

  public static CQ multiEdge() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), b);
    cq.addAtom(b, label(3), a);

    return cq;
  }

  public static CQ selfLoopBranch() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, label(1), a);
    cq.addAtom(a, label(2), b);
    cq.addAtom(b, label(3), c);

    return cq;
  }

  public static CQ twoStarsConnected() {
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

  private static Predicate label(int id) {
    return new Predicate(id, String.valueOf(id));
  }
}
