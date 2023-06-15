package com.ebremer.beakgraph.ng;



import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Insertion sorter.
 */
class InsertionSorter {

  /**
   * Sorts the range of a vector by insertion sort.
   *
   * @param vector     the vector to be sorted.
   * @param startIdx   the start index of the range (inclusive).
   * @param endIdx     the end index of the range (inclusive).
   * @param buffer     an extra buffer with capacity 1 to hold the current key.
   * @param comparator the criteria for vector element comparison.
   * @param <V>        the vector type.
   */
  static <V extends BaseFixedWidthVector> void insertionSort( V vector, int startIdx, int endIdx, VectorValueComparator<V> comparator, V buffer) {
    comparator.attachVectors(vector, buffer);
    for (int i = startIdx; i <= endIdx; i++) {
      buffer.copyFrom(i, 0, vector);
      int j = i - 1;
      while (j >= startIdx && comparator.compare(j, 0) > 0) {
        vector.copyFrom(j, j + 1, vector);
        j = j - 1;
      }
      vector.copyFrom(0, j + 1, buffer);
    }
  }

  /**
   * Sorts the range of vector indices by insertion sort.
   *
   * @param indices    the vector  indices.
   * @param startIdx   the start index of the range (inclusive).
   * @param endIdx     the end index of the range (inclusive).
   * @param comparator the criteria for vector element comparison.
   * @param <V>        the vector type.
   */
  static <V extends ValueVector> void insertionSort( IntVector indices, int startIdx, int endIdx, VectorValueComparator<V> comparator ) {
    for (int i = startIdx; i <= endIdx; i++) {
      int key = indices.get(i);
      int j = i - 1;
      while (j >= startIdx && comparator.compare(indices.get(j), key) > 0) {
        indices.set(j + 1, indices.get(j));
        j = j - 1;
      }
      indices.set(j + 1, key);
    }
  }
}
