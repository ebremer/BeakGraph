package com.ebremer.beakgraph.rdf;

import static com.ebremer.beakgraph.rdf.DualSort.ColumnOrder.OS;
import static com.ebremer.beakgraph.rdf.DualSort.ColumnOrder.SO;
import java.util.stream.IntStream;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.GeneralOutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class DualSort {
    
    public static enum ColumnOrder {SO,OS};

    public static VectorValueComparator<StructVector> getIIComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        IntVector child1 = structVector.getChild("o", IntVector.class);
        VectorValueComparator<IntVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compareNotNull(int index1, int index2) {
                if (order == SO) {
                    int result0 = childComp0.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp1.compare(index1, index2);
                } else {
                    int result0 = childComp1.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp0.compare(index1, index2);
                }
            }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
        return comp;
    }

    public static VectorValueComparator<StructVector> getILComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        BigIntVector child1 = structVector.getChild("o", BigIntVector.class);
        VectorValueComparator<BigIntVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compareNotNull(int index1, int index2) {
                if (order == SO) {
                    int result0 = childComp0.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp1.compare(index1, index2);
                } else {
                    int result0 = childComp1.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp0.compare(index1, index2);
                }
            }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
        return comp;
    }

    public static VectorValueComparator<StructVector> getIFComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        Float4Vector child1 = structVector.getChild("o", Float4Vector.class);
        VectorValueComparator<Float4Vector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compareNotNull(int index1, int index2) {
                if (order == SO) {
                    int result0 = childComp0.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp1.compare(index1, index2);
                } else {
                    int result0 = childComp1.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp0.compare(index1, index2);
                }
            }
            
            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
        return comp;
    }

    public static VectorValueComparator<StructVector> getISComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        VarCharVector child1 = structVector.getChild("o", VarCharVector.class);
        VectorValueComparator<VarCharVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compareNotNull(int index1, int index2) {
                if (order == SO) {
                    int result0 = childComp0.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp1.compare(index1, index2);
                } else {
                    int result0 = childComp1.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp0.compare(index1, index2);
                }
            }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
        return comp;
    }

    public static VectorValueComparator<StructVector> getIRComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        Float4Vector child1 = structVector.getChild("o", Float4Vector.class);
        VectorValueComparator<Float4Vector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compareNotNull(int index1, int index2) {
                if (order == SO) {
                    int result0 = childComp0.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp1.compare(index1, index2);
                } else {
                    int result0 = childComp1.compare(index1, index2);
                    if (result0 != 0) {
                        return result0;
                    }
                    return childComp0.compare(index1, index2);
                }
            }

            @Override
            public VectorValueComparator createNew() {
                return this;
            }
        };
        return comp;
    }

    public static void Sort(StructVector srcVector, StructVector destVector, ColumnOrder order) {
        GeneralOutOfPlaceVectorSorter<StructVector> sorter = new GeneralOutOfPlaceVectorSorter<>();
        VectorValueComparator<StructVector> comp =
            switch (srcVector.getChild("o")) {
                case IntVector o -> getIIComparator(srcVector, order);
                case BigIntVector o -> getILComparator(srcVector, order);
                case Float4Vector o -> getIFComparator(srcVector, order);
                case VarCharVector o -> getISComparator(srcVector, order);
                default -> throw new Error("can't handle this");
            };
        sorter.sortOutOfPlace(srcVector, destVector, comp);  
    }

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator();
        final int vectorLength = 7;
        StructVector src = StructVector.empty("src struct", allocator);
        IntVector srcChild0 = src.addOrGet("s", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
        IntVector srcChild1 = src.addOrGet("o", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
        ValueVectorDataPopulator.setVector(srcChild0, 2, 3, 2, 2, 7, 3, 6);
        ValueVectorDataPopulator.setVector(srcChild1, 9, 4, 4, 3, 9, 4, 6);
        IntStream.range(0, vectorLength).forEach(i -> src.setIndexDefined(i));
        src.setValueCount(vectorLength);
      
        StructVector top = StructVector.empty("dst", src.getAllocator());
        StructVector so = top.addOrGet("so", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        IntVector s = so.addOrGet("s", src.getChild("s").getField().getFieldType(), IntVector.class);
        s.allocateNew(vectorLength);
        IntVector o = so.addOrGet("o", src.getChild("s").getField().getFieldType(), IntVector.class);
        o.allocateNew(vectorLength);
        top.setValueCount(vectorLength);
        //StructVector os = top.addOrGet("os", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        System.out.println("LEN : "+src.getValueCount()+" "+so.getValueCount()+" "+so.getValueCapacity());
        Sort(src, so, OS);
        System.out.println("ORG -> "+src);
        System.out.println("FWD -> "+so);
        //Sort(src, os, OS);
        //System.out.println("RVR -> "+os);
    }
}
