package com.ebremer.beakgraph.ng;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.IntStream;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sorter for the indices of a vector.
 * @param <V> vector type.
 */
public class IndexSorter<V extends ValueVector> {
    static final int STOP_CHOOSING_PIVOT_THRESHOLD = 3;
    public static final int CHANGE_ALGORITHM_THRESHOLD_LARGE_DATA = 15;
    public static final int CHANGE_ALGORITHM_THRESHOLD = 15;
    private VectorValueComparator<V> comparator;
    private static final Logger logger = LoggerFactory.getLogger(IndexSorter.class);
    

    private class SortTask extends RecursiveTask<Void> {
        private final int low;
        private final int high;
        private final IntVector indices;
        private final int depth;

        SortTask(int low, int high, IntVector indices, int depth) {
            this.depth = depth + 1;
            this.low = low;
            this.high = high;
            this.indices = indices;
          //  System.out.println(depth+"  "+low+", "+high);
            
        }

        @Override
        protected Void compute() {
            if (isSorted(indices, low, high, comparator)) {
               // System.out.println(depth+"  "+low+", "+high+" Aleady sorted. Done.");
                return null;           
            } else if (low < high) {
                if ((high - low) < CHANGE_ALGORITHM_THRESHOLD_LARGE_DATA) {
                    InsertionSorter.insertionSort(indices, low, high, comparator);
                } else {
                    int mid = partition(low, high, indices, comparator);
                    SortTask left = new SortTask(low, mid, indices, depth);
                    SortTask right = new SortTask(mid + 1, high, indices, depth);
                    invokeAll(left,right);
                }
            }
          //  System.out.println(depth+"  "+low+", "+high+" Done.");
            return null;
        }
    }

    public void parallelSort(V vector, IntVector indices, VectorValueComparator<V> comparator) {
        comparator.attachVector(vector);
        IntStream.range(0, vector.getValueCount()).forEach(i -> indices.set(i, i));
        this.comparator = comparator;
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(new SortTask(0, indices.getValueCount() - 1, indices, 0));
    }
    
  /**
   * Vector indices to sort.
   */
  private IntVector indices;

  /**
   * Sorts indices, by quick-sort. Suppose the vector is denoted by v.
   * After calling this method, the following relations hold:
   * v(indices[0]) <= v(indices[1]) <= ...
   * @param vector the vector whose indices need to be sorted.
   * @param indices the vector for storing the sorted indices.
   * @param comparator the comparator to sort indices.
   */
    public void sort(V vector, IntVector indices, VectorValueComparator<V> comparator) {
        comparator.attachVector(vector);
        this.indices = indices;
        IntStream.range(0, vector.getValueCount()).forEach(i -> indices.set(i, i));
        this.comparator = comparator;
        quickSort();
    }
  
    private boolean isSorted(IntVector indices, int low, int high, VectorValueComparator<V> comparator) {
        for (int i = low; i < high; i++) {
            if (comparator.compare(indices.get(i), indices.get(i + 1)) > 0) {
                return false;
            }
        }
        return true;
    }


    private void quickSort() {
        try (OffHeapIntStack rangeStack = new OffHeapIntStack(indices.getAllocator())) {
            rangeStack.push(0);
            rangeStack.push(indices.getValueCount() - 1);

            while (!rangeStack.isEmpty()) {
                int high = rangeStack.pop();
                int low = rangeStack.pop();

                if (low < high) {
                    if (isSorted(indices, low, high, comparator)) {
                        continue;
                    }

                    if (high - low < CHANGE_ALGORITHM_THRESHOLD) {
                        InsertionSorter.insertionSort(indices, low, high, comparator);
                        continue;
                    }

                    int mid = partition(low, high, indices, comparator);

                    // push the larger part to stack first, to reduce the required stack size
                    if (high - mid < mid - low) {
                        rangeStack.push(low);
                        rangeStack.push(mid - 1);
                        rangeStack.push(mid + 1);
                        rangeStack.push(high);
                    } else {
                        rangeStack.push(mid + 1);
                        rangeStack.push(high);
                        rangeStack.push(low);
                        rangeStack.push(mid - 1);
                    }
                }
            }
        }
    }

  /**
   *  Select the pivot as the median of 3 samples.
   */
  static <T extends ValueVector> int choosePivot( int low, int high, IntVector indices, VectorValueComparator<T> comparator ) {
    // we need at least 3 items
    if (high - low + 1 < STOP_CHOOSING_PIVOT_THRESHOLD) {
      return indices.get(low);
    }

    int mid = low + (high - low) / 2;

    // find the median by at most 3 comparisons
    int medianIdx;
    if (comparator.compare(indices.get(low), indices.get(mid)) < 0) {
      if (comparator.compare(indices.get(mid), indices.get(high)) < 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(indices.get(low), indices.get(high)) < 0) {
          medianIdx = high;
        } else {
          medianIdx = low;
        }
      }
    } else {
      if (comparator.compare(indices.get(mid), indices.get(high)) > 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(indices.get(low), indices.get(high)) < 0) {
          medianIdx = low;
        } else {
          medianIdx = high;
        }
      }
    }
    
// hack
medianIdx = mid;

    // move the pivot to the low position, if necessary
    if (medianIdx != low) {
      int tmp = indices.get(medianIdx);
      indices.set(medianIdx, indices.get(low));
      indices.set(low, tmp);
      return tmp;
    } else {
      return indices.get(low);
    }
  }

  /**
   * Partition a range of values in a vector into two parts, with elements in one part smaller than
   * elements from the other part. The partition is based on the element indices, so it does
   * not modify the underlying vector.
   * @param low the lower bound of the range.
   * @param high the upper bound of the range.
   * @param indices vector element indices.
   * @param comparator criteria for comparison.
   * @param <T> the vector type.
   * @return the index of the split point.
   */
    public static <T extends ValueVector> int partition( int low, int high, IntVector indices, VectorValueComparator<T> comparator ) {
        int pivotIndex = choosePivot(low, high, indices, comparator);
        while (low < high) {
            while (low < high && comparator.compare(indices.get(high), pivotIndex) >= 0) {
                high -= 1;
            }
            indices.set(low, indices.get(high));

            while (low < high && comparator.compare(indices.get(low), pivotIndex) <= 0) {
                low += 1;
            }
            indices.set(high, indices.get(low));
        }        
        indices.set(low, pivotIndex);
        return low;
    }
}
