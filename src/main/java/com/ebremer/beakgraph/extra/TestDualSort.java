package com.ebremer.beakgraph.extra;

import java.util.stream.IntStream;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.GeneralOutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestDualSort {

    public static VectorValueComparator<StructVector> getComparator(StructVector structVector) {
        IntVector child0 = structVector.getChild("column0", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        IntVector child1 = structVector.getChild("column1", IntVector.class);
        VectorValueComparator<IntVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {    
            @Override
            public int compareNotNull(int index1, int index2) {
                int result0 = childComp0.compare(index1, index2);
                if (result0 != 0) {
                  return result0;
                }
                return childComp1.compare(index1, index2);
              }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
            };
        return comp;
    }

  public static void main(String[] args) {
    BufferAllocator allocator = new RootAllocator(1024 * 1024);
    final int vectorLength = 7;
    try (StructVector srcVector = StructVector.empty("src struct", allocator);
         StructVector dstVector = StructVector.empty("dst struct", allocator)) {
        // define src and dest
      IntVector srcChild0 = srcVector.addOrGet("column0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector srcChild1 = srcVector.addOrGet("column1", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector dstChild0 = dstVector.addOrGet("column0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector dstChild1 = dstVector.addOrGet("column1", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      // populate src
      ValueVectorDataPopulator.setVector(srcChild0, 2, 3, 2, 2, 7, 3, 6);
      ValueVectorDataPopulator.setVector(srcChild1, 9, 4, 4, 3, 9, 4, 6);
      IntStream.range(0, vectorLength).forEach(i -> srcVector.setIndexDefined(i));
      srcVector.setValueCount(vectorLength);
      
      // size destination
      dstChild0.allocateNew(vectorLength);
      dstChild1.allocateNew(vectorLength);
      dstVector.setValueCount(vectorLength);
      VectorValueComparator<StructVector> comp = getComparator(srcVector);
      
      GeneralOutOfPlaceVectorSorter<StructVector> sorter = new GeneralOutOfPlaceVectorSorter<>();
      sorter.sortOutOfPlace(srcVector, dstVector, comp);
      
      
      System.out.println(srcVector);
      System.out.println(dstVector);
    }
  }
}
