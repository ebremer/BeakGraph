package com.ebremer.beakgraph.ng;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.OutOfPlaceVectorSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

public class BGStructVectorSorter<StructVector extends ValueVector> implements OutOfPlaceVectorSorter<StructVector> {

    @Override
    public void sortOutOfPlace(StructVector srcVector, StructVector dstVector, VectorValueComparator<StructVector> comparator) {
        comparator.attachVector(srcVector);
        try (IntVector sortedIndices = new IntVector("", srcVector.getAllocator())) {
            sortedIndices.allocateNew(srcVector.getValueCount());
            sortedIndices.setValueCount(srcVector.getValueCount());
            IndexSorter<StructVector> indexSorter = new IndexSorter<>();
            indexSorter.sort(srcVector, sortedIndices, comparator);


            org.apache.arrow.vector.complex.StructVector src = (org.apache.arrow.vector.complex.StructVector) srcVector;
            IntVector srcs = (IntVector) src.getChild("s");                
            ArrowBuf srcsb = srcs.getDataBuffer();
                
            org.apache.arrow.vector.complex.StructVector dest = (org.apache.arrow.vector.complex.StructVector) dstVector;
            IntVector dests = (IntVector) dest.getChild("s");                
            ArrowBuf  destsb = dests.getDataBuffer();
            int srcsw = srcs.getTypeWidth();            
                
            ValueVector srco = (ValueVector) src.getChild("o");
            ArrowBuf srcob = srco.getDataBuffer();
                
            ValueVector desto = (ValueVector) src.getChild("o");
            ArrowBuf  destob = desto.getDataBuffer();
            int srcow = comparator.getValueWidth() - srcsw;

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                int totalElements = sortedIndices.getValueCount();
                int n = Runtime.getRuntime().availableProcessors();
                int chunkSize = sortedIndices.getValueCount()/Runtime.getRuntime().availableProcessors();
                for (int i = 0; i < n; i++) {
                    final int start = i * chunkSize;
                    final int end = Math.min(start + chunkSize, totalElements);
                    executor.submit(() -> {
                        for (int X = start; X < end; X++) {
                            int srcIndex = sortedIndices.get(X);
                            final int dstIndex = X;                                              
                            MemoryUtil.copyMemory( srcsb.memoryAddress() + srcIndex * srcsw, destsb.memoryAddress() + dstIndex * srcsw, srcsw);
                            MemoryUtil.copyMemory( srcob.memoryAddress() + srcIndex * srcow, destob.memoryAddress() + dstIndex * srcow, srcow);
                        }
                    });
                }
            }
        }
    }
}
