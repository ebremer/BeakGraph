package com.ebremer.beakgraph.ng;


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;

/**
 * An off heap implementation of stack with int elements.
 */
class OffHeapIntStack implements AutoCloseable {

  private static final int INIT_SIZE = 128;

  private final IntVector intVector;

  private int top = 0;

  public OffHeapIntStack(BufferAllocator allocator) {
    intVector = new IntVector("int stack inner vector", allocator);
    intVector.allocateNew(INIT_SIZE);
    intVector.setValueCount(INIT_SIZE);
  }

  public void push(int value) {
    if (top == intVector.getValueCount()) {
      int targetCapacity = intVector.getValueCount() * 2;
      while (intVector.getValueCapacity() < targetCapacity) {
        intVector.reAlloc();
      }
      intVector.setValueCount(targetCapacity);
    }

    intVector.set(top++, value);
  }

  public int pop() {
    return intVector.get(--top);
  }

  public int getTop() {
    return intVector.get(top - 1);
  }

  public boolean isEmpty() {
    return top == 0;
  }

  public int getCount() {
    return top;
  }

  @Override
  public void close() {
    intVector.close();
  }
}
