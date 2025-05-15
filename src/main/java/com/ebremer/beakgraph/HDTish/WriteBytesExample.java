package com.ebremer.beakgraph.HDTish;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.security.SecureRandom;

public class WriteBytesExample {
    public static void main(String[] args) {
        byte[] bytes = new byte[100];   
        SecureRandom sec = new SecureRandom();
        sec.nextBytes(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        try (WritableHdfFile hdfFile = HdfFile.write(Paths.get("/data/j1.h5"))) {
            hdfFile.putDataset("randomdata", bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
