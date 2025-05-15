package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.DualSort.ColumnOrder.SO;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.GeneralOutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;

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
            public int getValueWidth() {
                return child0.getTypeWidth()+child1.getTypeWidth();
            }
            
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
            @Override
            public int compareNotNull(int index1, int index2) {
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
            public int getValueWidth() {
                return child0.getTypeWidth()+child1.getTypeWidth();
            }
            
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
            @Override
            public int compareNotNull(int index1, int index2) {
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
            public int getValueWidth() {
                return child0.getTypeWidth()+child1.getTypeWidth();
            }
            
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
            @Override
            public int compareNotNull(int index1, int index2) {
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
            public int getValueWidth() {
                return child0.getTypeWidth()+child1.getTypeWidth();
            }
            
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
            @Override
            public int compareNotNull(int index1, int index2) {
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
            public int getValueWidth() {
                return child0.getTypeWidth()+child1.getTypeWidth();
            }
            
            @Override
            public int compare(int index1, int index2) {
                return compareNotNull(index1, index2);
            }
            
            @Override
            public int compareNotNull(int index1, int index2) {
                switch (order) {
                    case SO:
                        int resultS0 = childComp0.compareNotNull(index1, index2);
                        if (resultS0 != 0) {
                            return resultS0;
                        }
                        return childComp1.compareNotNull(index1, index2);
                    case OS:
                        int result0S = childComp1.compareNotNull(index1, index2);
                        if (result0S != 0) {
                            return result0S;
                        }
                        return childComp0.compareNotNull(index1, index2);
                    default:
                        throw new IllegalArgumentException("Unknown order: " + order);    
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
        Object aa = srcVector.getChild("o");
        if (aa instanceof IntVector) {
            Yah<StructVector> sorter = new Yah<>();
            VectorValueComparator<StructVector> comp = getIIComparator(srcVector, order);
            sorter.sortOutOfPlace(srcVector, destVector, comp);
        } else if (aa instanceof BigIntVector) {
            Yah<StructVector> sorter = new Yah<>();
            VectorValueComparator<StructVector> comp = getILComparator(srcVector, order);
            sorter.sortOutOfPlace(srcVector, destVector, comp);
        } else if (aa instanceof Float4Vector) {
            Yah<StructVector> sorter = new Yah<>();
            VectorValueComparator<StructVector> comp = getIFComparator(srcVector, order);
            sorter.sortOutOfPlace(srcVector, destVector, comp);
        } else if (aa instanceof Float8Vector) {
            Yah<StructVector> sorter = new Yah<>();
            VectorValueComparator<StructVector> comp = getIDComparator(srcVector, order);
            sorter.sortOutOfPlace(srcVector, destVector, comp);
        } else if (aa instanceof VarCharVector) {
            Yah<StructVector> sorter = new Yah<>();
            VectorValueComparator<StructVector> comp = getISComparator(srcVector, order);
            sorter.sortOutOfPlace(srcVector, destVector, comp);
        } else {
            throw new Error("can't handle this");
        }          
    }
}
