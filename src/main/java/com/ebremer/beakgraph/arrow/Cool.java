/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.arrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.arrow.algorithm.search.VectorRangeSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

/**
 *
 * @author erich
 */
public class Cool {
    
    private static void writeStructVector(NullableStructWriter writer, int value1, long value2) {
        writer.start();
        writer.integer("f0").writeInt(value1);
        writer.bigInt("f1").writeBigInt(value2);
        writer.end();
    }
    
    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final StructVector ss = StructVector.empty("struct", allocator);
        ss.addOrGet("f0", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        ss.addOrGet("f1", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
        NullableStructWriter writer1 = ss.getWriter();
        writeStructVector(writer1, 1, 101L);
        writeStructVector(writer1, 2, 92L);
        writeStructVector(writer1, 33, 83L);
        writeStructVector(writer1, 4, 74L);
        writeStructVector(writer1, 5, 55L);
        writer1.setValueCount(5);
        System.out.println(ss);
        System.out.println(ss.getChild("f1"));
        System.out.println(ss.getChild("f1").getField().getType());
        BigIntVector notsorted = (BigIntVector) ss.getChild("f1");
        IndexSorter<BigIntVector> indexSorter = new IndexSorter<>();
        DefaultVectorComparators.LongComparator comp = new DefaultVectorComparators.LongComparator();
        IntVector idx = new IntVector("", notsorted.getAllocator());
        idx.setValueCount(notsorted.getValueCount());
        indexSorter.sort(notsorted, idx, comp);
        System.out.println(idx+" ===("+idx.getName()+")");
        BigIntVector biv = new BigIntVector("haha", notsorted.getAllocator());
        biv.setValueCount(notsorted.getValueCount());
        IntStream.range(0, notsorted.getValueCount()).forEach(i -> {
            //biv.set(i, notsorted.get(idx.get(i)));
            long t = notsorted.get(i);
            notsorted.set(i, notsorted.get(idx.get(i)));
            notsorted.set(idx.get(i), t);
        });
        System.out.println("-----------------------------");
        System.out.println(idx);
        System.out.println(biv);
        System.out.println(notsorted);
        System.out.println("-----------------------------");
        
        IntVector vector = new IntVector("intVector", allocator);
        for (int i = 0; i < 10; i++) {
            vector.setSafe(i, 10-i);
        }
        vector.setValueCount(10);

        //TransferPair tp = vector.getTransferPair(allocator);
        //tp.splitAndTransfer(0, 5);
        //IntVector sliced = (IntVector) tp.getTo();
        System.out.println(vector);
        //System.out.println(sliced);
// In this case, the vector values are [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] and the sliceVector values are [0, 1, 2, 3, 4].
        System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        System.out.println(ss);
        
        final int maxValue = 100;
        int repeat = 3;
        try (IntVector intVector = new IntVector("int vec", allocator)) {
            intVector.allocateNew(maxValue * repeat);
            intVector.setValueCount(maxValue * repeat);
            for (int i = 0; i < maxValue; i++) {
                for (int j = 0; j < repeat; j++) {
                    if (i == 0) {
                        intVector.setNull(i * repeat + j);
                    } else {
                        intVector.set(i * repeat + j, i);
                    }
                }
            }
        System.out.println(intVector);
        VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
        for (int i = 0; i < maxValue; i++) {
            int low = VectorRangeSearcher.getFirstMatch(intVector, comparator, intVector, i * repeat);
            int high = VectorRangeSearcher.getLastMatch(intVector, comparator, intVector, i * repeat);
            TransferPair tp = intVector.getTransferPair(allocator);
            tp.splitAndTransfer(low, high-low+1);
            try (IntVector sliced = (IntVector) tp.getTo()) {
                System.out.println((i * repeat) +" ==== "+ low+"  "+high + " ---> "+sliced);
            }  
        }
    }
        
      //  VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
       // VectorRangeSearcher.getFirstMatch(intVector, comparator, intVector, i * repeat);
       
        Field name = new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Schema schemaPerson = new Schema(asList(name, age));
        VarCharVector nameVector;
        IntVector ageVector;
        try(
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
        ){
            nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(10);
            nameVector.set(0, "Zero".getBytes());
            nameVector.set(1, "One".getBytes());
            nameVector.set(2, "Two".getBytes());
            nameVector.set(3, "Three".getBytes());
            nameVector.set(4, "Four".getBytes());
            nameVector.set(5, "Five".getBytes());
            nameVector.set(6, "Six".getBytes());
            nameVector.set(7, "Seven".getBytes());
            nameVector.set(8, "Eight".getBytes());
            nameVector.set(9, "Nine".getBytes());
            ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(10);
            ageVector.set(0, 0);
            ageVector.set(1, 10);
            ageVector.set(2, 20);
            ageVector.set(3, 30);
            ageVector.set(4, 40);
            ageVector.set(5, 50);
            ageVector.set(6, 60);
            ageVector.set(7, 70);
            ageVector.set(8, 80);
            ageVector.set(9, 90);
            vectorSchemaRoot.setRowCount(10);

        VectorSchemaRoot root = new VectorSchemaRoot(List.of(ss.getField(), ageVector.getField(), nameVector.getField()), List.of(ss, ageVector, nameVector));
        File file = new File("random.arrow");
        try (
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + root.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        try(
            BufferAllocator rootAllocator = new RootAllocator();
            FileInputStream fileInputStream = new FileInputStream(file);
            ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                System.out.println("OFFSET ---> "+arrowBlock.getOffset()+"  "+arrowBlock.getBodyLength());
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        }
        
    }
    
}
