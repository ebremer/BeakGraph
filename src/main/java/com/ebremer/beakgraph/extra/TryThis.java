/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.extra;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import static java.util.Arrays.asList;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 *
 * @author erich
 */
public class TryThis {
    
    public static void main(String[] args) {
try (BufferAllocator allocator = new RootAllocator()) {
    Field name = new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    try(
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
    ){
        VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
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
        
        IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
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
        File file = new File("random.arrow");
        try (
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.writeBatch();
            writer.writeBatch();
            writer.end();
            System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

VectorSchemaRoot[] yay = new VectorSchemaRoot[5];
int c = 0;

File file = new File("random.arrow");
try(
    BufferAllocator rootAllocator = new RootAllocator();
    FileInputStream fileInputStream = new FileInputStream(file);
    ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
){
    System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
    for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        System.out.println("OFFSET ---> "+arrowBlock.getOffset()+"  "+arrowBlock.getBodyLength());
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot vbr = reader.getVectorSchemaRoot();
        System.out.print(vbr.contentToTSVString());
        yay[c] = cloneRoot(vbr, rootAllocator);
        c++;
    }
    
        System.out.println("============================================================================================");
    
    IntStream.range(0, c).forEach(y->{
        System.out.println(yay[y].contentToTSVString());
        yay[y].close();
    });
    
} catch (IOException e) {
    e.printStackTrace();
}


    }
    
    private static VectorSchemaRoot cloneRoot(VectorSchemaRoot originalRoot, BufferAllocator allocator) {
        VectorSchemaRoot theRoot = VectorSchemaRoot.create(originalRoot.getSchema(), allocator);
        VectorLoader loader = new VectorLoader(theRoot);
        VectorUnloader unloader = new VectorUnloader(originalRoot);
        try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
            loader.load(recordBatch);
        }
        return theRoot;
    }  
    
}
