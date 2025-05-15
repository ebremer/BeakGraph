package com.ebremer.beakgraph.arrow;

import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.FixedWidthOutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.OutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 *
 * @author erich
 */
public class Tools {
    
    public Tools() {
        
    }
    
    public static Pair Sort(String i, Pair pair) {
        try (
            BufferAllocator allocator = new RootAllocator();
            IntVector intVectorNotSorted = new IntVector("intvectornotsorted", allocator);
            IntVector sorted = (IntVector) intVectorNotSorted.getField().getFieldType().createNewSingleVector("new-out-of-place-sorter",allocator, null);
        ) {
            OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter = new FixedWidthOutOfPlaceVectorSorter<>();
            VectorValueComparator<IntVector> comparatorOutOfPlaceSorter = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
            sorted.allocateNew(intVectorNotSorted.getValueCount());
            sorted.setValueCount(intVectorNotSorted.getValueCount());
            sorterOutOfPlaceSorter.sortOutOfPlace(intVectorNotSorted, sorted, comparatorOutOfPlaceSorter);
return null;
        
        }
    }
}
