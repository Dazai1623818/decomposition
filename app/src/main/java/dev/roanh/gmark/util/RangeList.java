package dev.roanh.gmark.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Simple indexed list used by the native CPQ index to keep predicate references addressable by ID.
 * The implementation is intentionally lightweight; it only provides the operations exercised by the
 * native index loader.
 */
public final class RangeList<T> implements Iterable<T> {
  private final List<T> entries;

  public RangeList(int size) {
    this(size, () -> null);
  }

  public RangeList(int size, Supplier<T> initializer) {
    Objects.requireNonNull(initializer, "initializer");
    this.entries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      entries.add(initializer.get());
    }
  }

  public T get(IDable idable) {
    return get(idable.getID());
  }

  /**
   * Compatibility overload for callers that use the {@code dev.roanh.gmark.type.IDable} interface
   * directly (e.g. classes from the original gMark distribution).
   */
  public T get(dev.roanh.gmark.type.IDable idable) {
    return get(idable.getID());
  }

  public T get(int index) {
    ensureCapacity(index + 1);
    return entries.get(index);
  }

  public void set(int index, T value) {
    ensureCapacity(index + 1);
    entries.set(index, value);
  }

  public void set(IDable idable, T value) {
    set(idable.getID(), value);
  }

  /**
   * Compatibility overload for callers that use the {@code dev.roanh.gmark.type.IDable} interface
   * directly (e.g. classes from the original gMark distribution).
   */
  public void set(dev.roanh.gmark.type.IDable idable, T value) {
    set(idable.getID(), value);
  }

  public int size() {
    return entries.size();
  }

  public void forEachNonNull(Consumer<T> consumer) {
    Objects.requireNonNull(consumer, "consumer");
    for (T entry : entries) {
      if (entry != null) {
        consumer.accept(entry);
      }
    }
  }

  @Override
  public Iterator<T> iterator() {
    return entries.iterator();
  }

  private void ensureCapacity(int desiredSize) {
    while (entries.size() < desiredSize) {
      entries.add(null);
    }
  }
}
