package com.ebremer.beakgraph.ng;

import org.apache.arrow.algorithm.sort.OutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An out-of-place sorter for vectors of arbitrary type, with time complexity O(n*log(n)).
 * Since it does not make any assumptions about the memory layout of the vector, its performance
 * can be sub-optimal. So if another sorter is applicable ({@link FixedWidthInPlaceVectorSorter}),
 * it should be used in preference.
 *
 * @param <V> vector type.
 */
public class Yah<V extends ValueVector> implements OutOfPlaceVectorSorter<V> {
    
    private static final Logger logger = LoggerFactory.getLogger(Yah.class);

    @Override
    public void sortOutOfPlace(V srcVector, V dstVector, VectorValueComparator<V> comparator) {
        StopWatch sw = StopWatch.getInstance();
        comparator.attachVector(srcVector);

        try (IntVector sortedIndices = new IntVector("", srcVector.getAllocator())) {
            logger.trace(sw.Lapse("start sortOutOfPlace"));
            sw.reset();
            sortedIndices.allocateNew(srcVector.getValueCount());
            sortedIndices.setValueCount(srcVector.getValueCount());
            IndexSorter<V> indexSorter = new IndexSorter<>();
            if (srcVector.getValueCount()>1000000) {              
                try {
                    indexSorter.parallelSort(srcVector, sortedIndices, comparator);
                } catch (Exception e) {
                    System.out.println("Unexpected!!!!!!!!!!!!!!!!! " +e.toString());
                } catch (Throwable t) {
                   System.out.println("Really Unexpected!!!!!!!!!!!!!!!!! " +t.toString());
                }                          
            } else {
                indexSorter.sort(srcVector, sortedIndices, comparator);
            }
            sw.reset();

            for (int dstIndex = 0; dstIndex < sortedIndices.getValueCount(); dstIndex++) {
                int srcIndex = sortedIndices.get(dstIndex);
                dstVector.copyFromSafe(srcIndex, dstIndex, srcVector);
            }
        }
    }
}
