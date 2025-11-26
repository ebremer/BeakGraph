package com.ebremer.beakgraph.netcdt4;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.nc2.write.NetcdfFormatWriter;
import ucar.nc2.write.NetcdfFileFormat;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;

import java.io.IOException;
import java.util.Random;

/**
 * Creates a NetCDF-4 file containing a large, chunked, and compressed
 * array of random long integers.
 */
public class WriteChunkedNetCdf {

    // Define the size of the array
    private static final int DATA_SIZE = 200_000_000;
    
    // Define the output filename
    private static final String FILENAME = "random_longs.h5";

    public static void main(String[] args) {
        System.out.println("Starting NetCDF write process...");

        // 1. Generate the random data
        long[] data = generateRandomLongs(DATA_SIZE);

        // 2. Write the data to a NetCDF file
        try {
            writeNetCdfFile(data);
            System.out.println("Successfully wrote data to " + FILENAME);
        } catch (IOException | InvalidRangeException e) {
            System.err.println("Error writing NetCDF file:");
            e.printStackTrace();
        }
    }

    /**
     * Generates an array of random long values.
     * @param size The number of random longs to generate.
     * @return An array of longs.
     */
    private static long[] generateRandomLongs(int size) {
        System.out.println("Generating " + size + " random longs...");
        long[] data = new long[size];
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            data[i] = random.nextLong();
        }
        System.out.println("Data generation complete.");
        return data;
    }

    /**
     * Writes the data array to a chunked and compressed NetCDF-4 file.
     * @param data The long array to write.
     * @throws IOException If an I/O error occurs.
     * @throws InvalidRangeException If the write range is invalid.
     */
    private static void writeNetCdfFile(long[] data) throws IOException, InvalidRangeException {
        
        // 1. Define the chunking and compression strategy
        //    This is the correct method as per the Unidata documentation
        int deflateLevel = 5;
        boolean shuffle = true; // Shuffle is good for compression
        
        // Use the standard chunking strategy
        Nc4Chunking chunker = Nc4ChunkingStrategy.factory(
                Nc4Chunking.Strategy.standard, deflateLevel, shuffle
        );

        // 2. Create the builder using createNewNetcdf4, passing in the chunker
        NetcdfFormatWriter.Builder builder = NetcdfFormatWriter.createNewNetcdf4(
                NetcdfFileFormat.NETCDF4, FILENAME, chunker
        );

        // 3. Define the dimension and add it to the builder
        String dimName = "data_dim";
        Dimension dim = builder.addDimension(dimName, DATA_SIZE);

        // 4. Define the variable and add it to the builder
        String varName = "random_data";
        
        // We no longer need to (and can't) set chunking/deflate on the var builder
        // The Nc4Chunking object handles this for all variables in the file.
        builder.addVariable(varName, DataType.LONG, dimName);

        // 5. Build the writer and write the data in a try-with-resources block
        try (NetcdfFormatWriter writer = builder.build()) {
            
            // The variable is now part of the writer. Find it by name.
            Variable dataVar = writer.findVariable(varName); 
            if (dataVar == null) {
                throw new IOException("Failed to find variable " + varName + " after building writer.");
            }

            System.out.println("Writing data to file...");

            // Create a ucar.ma2.Array from the primitive Java array
            Array dataArray = Array.factory(DataType.LONG, new int[]{DATA_SIZE}, data);

            // Write the data to the variable
            // We write starting at origin (index 0)
            int[] origin = new int[]{0};
            writer.write(dataVar, origin, dataArray);

            System.out.println("File writing complete.");
        } // The writer is automatically closed here
    }
}