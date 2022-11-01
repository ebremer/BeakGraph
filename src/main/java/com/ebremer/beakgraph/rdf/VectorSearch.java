/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.rdf;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

/**
 * Search for the range of a particular element in the target vector.
 */
public class VectorSearch {
        
    /**
    * Search for the first occurrence of an element.
    * The search is based on the binary search algorithm. So the target vector must be sorted.
    * @param targetVector the vector from which to perform the search.
    * @param comparator the criterion for the comparison.
    * @param keyVector the vector containing the element to search.
    * @param keyIndex the index of the search key in the key vector.
    * @param <V> the vector type.
    * @return the index of the first matched element if any, and -1 otherwise.
    */
    public static <V extends ValueVector> int getFirstOrClosestMatch(V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
        comparator.attachVectors(keyVector, targetVector);
        int mid = -1;
        int low = 0;
        int high = targetVector.getValueCount() - 1;
        while (low <= high) {
            mid = low + (high - low) / 2;
            V ss = (V) targetVector;
            V key = (V) keyVector;
            //System.out.println(key.get(keyIndex)+"  LOW SCAN ==> low : "+low+" = ["+ss.get(low)+"] mid :"+mid+"=["+ss.get(mid)+"] high : "+high +" = ["+ss.get(high)+"]");
            int result = comparator.compare(keyIndex, mid);
            if (result < 0) {
                // the key is smaller
                high = mid - 1;
            } else if (result > 0) {
                // the key is larger
                low = mid + 1;
            } else {
                // an equal element is found
                // continue to go left-ward
                high = mid - 1;
            }
        }
        //System.out.println("DONE SEARCH : "+mid);
        return mid;
    }
    
      /**
   * Search for the last occurrence of an element.
   * The search is based on the binary search algorithm. So the target vector must be sorted.
   * @param targetVector the vector from which to perform the search.
   * @param comparator the criterion for the comparison.
   * @param keyVector the vector containing the element to search.
   * @param keyIndex the index of the search key in the key vector.
   * @param <V> the vector type.
   * @return the index of the last matched element if any, and -1 otherwise.
   */
    public static <V extends ValueVector> int getFirstOrLargestRight(V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
        comparator.attachVectors(keyVector, targetVector);
        int mid = -1;
        int low = 0;
        int high = targetVector.getValueCount() - 1;
        //System.out.println("LOW/HIGH : "+low+" "+high);
        while (low <= high) {
            mid = low + (high - low) / 2;
            V ss = (V) targetVector;
            V key = (V) keyVector;
           // System.out.println(key.get(keyIndex)+"  HIGH SCAN ==> low : "+low+" = ["+ss.get(low)+"] mid :"+mid+"=["+ss.get(mid)+"] high : "+high +" = ["+ss.get(high)+"]");
            int result = comparator.compare(keyIndex, mid);
            if (result < 0) {
                // the key is smaller
                high = mid - 1;
            } else if (result > 0) {
                // the key is larger
                low = mid + 1;
            } else {
                // an equal element is found,
                // continue to go right-ward
                low = mid + 1;
            }
        }
        return mid;
    }
}
