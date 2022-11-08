/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.rdf;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.memory.RootAllocator;

/**
 *
 * @author erich
 */
public class BLAM {
    
    public static void main(String[] args) {
        try(
            BufferAllocator allocator = new RootAllocator();
            IntVector binarySearchVector = new IntVector("", allocator);
        ) {
            binarySearchVector.allocateNew(10);
            binarySearchVector.setValueCount(10);
            for (int i = 0; i < 10; i++) {
                binarySearchVector.set(i, i);
            }
            VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(binarySearchVector);
            int result = VectorSearcher.binarySearch(binarySearchVector, comparatorInt, binarySearchVector, 3);

            System.out.println(result);
        }
    }
}
