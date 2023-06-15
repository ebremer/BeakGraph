package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.DualSort.ColumnOrder.SO;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.GeneralOutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class DualSort {
    
    public static enum ColumnOrder {SO,OS};

    public VectorValueComparator<StructVector> getIIComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        IntVector child1 = structVector.getChild("o", IntVector.class);
        VectorValueComparator<IntVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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

    public VectorValueComparator<StructVector> getILComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        BigIntVector child1 = structVector.getChild("o", BigIntVector.class);
        VectorValueComparator<BigIntVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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

    public VectorValueComparator<StructVector> getIFComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        Float4Vector child1 = structVector.getChild("o", Float4Vector.class);
        VectorValueComparator<Float4Vector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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
    
    public VectorValueComparator<StructVector> getIDComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        Float8Vector child1 = structVector.getChild("o", Float8Vector.class);
        VectorValueComparator<Float8Vector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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

    public VectorValueComparator<StructVector> getISComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        VarCharVector child1 = structVector.getChild("o", VarCharVector.class);
        VectorValueComparator<VarCharVector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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

    public VectorValueComparator<StructVector> getIRComparator(StructVector structVector, ColumnOrder order) {
        IntVector child0 = structVector.getChild("s", IntVector.class);
        VectorValueComparator<IntVector> childComp0 = DefaultVectorComparators.createDefaultComparator(child0);
        childComp0.attachVector(child0);
        Float4Vector child1 = structVector.getChild("o", Float4Vector.class);
        VectorValueComparator<Float4Vector> childComp1 = DefaultVectorComparators.createDefaultComparator(child1);
        childComp1.attachVector(child1);
        VectorValueComparator<StructVector> comp = new VectorValueComparator<StructVector>() {
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
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

    public void Sort(StructVector srcVector, StructVector destVector, ColumnOrder order) {
        GeneralOutOfPlaceVectorSorter<StructVector> sorter = new GeneralOutOfPlaceVectorSorter<>();
        VectorValueComparator<StructVector> comp;
        Object aa = srcVector.getChild("o");
        if (aa instanceof IntVector) {
            comp = getIIComparator(srcVector, order);
        } else if (aa instanceof BigIntVector) {
            comp = getILComparator(srcVector, order);
        } else if (aa instanceof Float4Vector) {
            comp = getIFComparator(srcVector, order);
        } else if (aa instanceof Float8Vector) {
            comp = getIDComparator(srcVector, order);
        } else if (aa instanceof VarCharVector) {
            comp = getISComparator(srcVector, order);
        } else {
            throw new Error("can't handle this");
        }
        sorter.sortOutOfPlace(srcVector, destVector, comp);  
    }

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator();
        final int vectorLength = 1;
        StructVector src = StructVector.empty("src struct", allocator);
        IntVector srcChild0 = src.addOrGet("s", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        IntVector srcChild1 = src.addOrGet("o", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        System.out.println("Initialize Vectors");
        IntStream.range(0, vectorLength).forEach(i->{
            srcChild0.setSafe(i, vectorLength-i);
            srcChild1.setSafe(i, ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE));
        });  
        IntStream.range(0, vectorLength).forEach(i -> src.setIndexDefined(i));

        src.setValueCount(vectorLength);
/*
        DualSort dualsort = new DualSort();
        StructVector top = StructVector.empty("dst", src.getAllocator());
        StructVector so = top.addOrGet("so", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        IntVector s = so.addOrGet("s", src.getChild("s").getField().getFieldType(), IntVector.class);
        s.allocateNew(vectorLength);
        IntVector o = so.addOrGet("o", src.getChild("s").getField().getFieldType(), IntVector.class);
        o.allocateNew(vectorLength);
        top.setValueCount(vectorLength);
        //StructVector os = top.addOrGet("os", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        System.out.println("LEN : "+src.getValueCount()+" "+so.getValueCount()+" "+so.getValueCapacity());
              System.out.println("Sort Vectors");
        long x1 = System.nanoTime();
        dualsort.Sort(src, so, OS);
        System.out.println(System.nanoTime()-x1);
        System.out.println("ORG -> "+src);
        System.out.println("FWD -> "+so);
        x1 = System.nanoTime();
        dualsort.Sort(src, so, SO);
        System.out.println(System.nanoTime()-x1);
        System.out.println("RVR -> "+so);
        */
        System.out.println("ORG -> "+src);
        System.out.println("reset Vectors");
        final int vectorLength2 = 5;
        src.allocateNew();
        IntStream.range(0, vectorLength2).forEach(i->{
            srcChild0.setSafe(i, vectorLength2-i);
            srcChild1.setSafe(i, ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE));
        });     
        IntStream.range(0, vectorLength2).forEach(i -> src.setIndexDefined(i));
        src.setValueCount(vectorLength2);
        
        System.out.println("2ND -> "+src);
    }
}
