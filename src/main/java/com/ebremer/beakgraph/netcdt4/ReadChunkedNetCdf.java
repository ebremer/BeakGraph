package com.ebremer.beakgraph.netcdt4;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.Random;

/**
 * Performs a random read test on the NetCDF file created by WriteChunkedNetCdf.
 */
public class ReadChunkedNetCdf {

    // Constants must match the writer class
    private static final int DATA_SIZE = 20_000_000;
    private static final String FILENAME = "random_longs.h5";
    private static final String VAR_NAME = "random_data";
    
    // Define number of random reads for testing
    private static final int NUM_RANDOM_READS = 50_000;

    public static void main(String[] args) {
        try {
            readNetCdfFile();
        } catch (IOException | InvalidRangeException e) {
            System.err.println("Error during NetCDF read operation:");
            e.printStackTrace();
        }
    }

    /**
     * Reads a specified number of random single long values from the NetCDF file
     * and reports the read speed.
     * @throws IOException If an I/O error occurs.
     * @throws InvalidRangeException If the read range is invalid.
     */
    private static void readNetCdfFile() throws IOException, InvalidRangeException {
        System.out.println("\nStarting random read test...");
        System.out.println("Performing " + NUM_RANDOM_READS + " single long reads from " + FILENAME);

        Random random = new Random();
        int[] origin = new int[1];    // Origin array for reading
        int[] shape = new int[]{1};   // Shape array: we want to read 1 element
        long startTime = 0;
        long totalReads = 0; // Use long for safety, though int is fine

        // Open the file for reading using try-with-resources
        try (NetcdfFile ncfile = NetcdfFiles.open(FILENAME)) {
            
            Variable dataVar = ncfile.findVariable(VAR_NAME);
            if (dataVar == null) {
                throw new IOException("Failed to find variable " + VAR_NAME + " in file " + FILENAME);
            }

            // Basic check to ensure variable dimensions match our expectation
            if (dataVar.getRank() != 1 || dataVar.getSize() != DATA_SIZE) {
                 throw new IOException("Variable " + VAR_NAME + " has unexpected dimensions or size.");
            }

            // Start the timer just before the read loop
            startTime = System.nanoTime(); 

            for (int i = 0; i < NUM_RANDOM_READS; i++) {
                // Pick a random index in the array
                int randomIndex = random.nextInt(DATA_SIZE);
                origin[0] = randomIndex;

                // Read the single long value at that index
                Array readArray = dataVar.read(origin, shape);
                
                // We read the value to ensure the I/O operation isn't optimized away,
                // but we don't need to store it.
                readArray.getLong(0); 
                totalReads++;
            }

        } // The NetcdfFile is automatically closed here

        long endTime = System.nanoTime();
        
        // Calculate and report the results
        double durationNanos = (double)(endTime - startTime);
        double durationSeconds = durationNanos / 1_000_000_000.0;
        double readsPerSecond = totalReads / durationSeconds;

        System.out.println("Random read test complete.");
        System.out.println("Performed " + totalReads + " reads.");
        System.out.printf("Total read time: %.4f seconds%n", durationSeconds);
        System.out.printf("Read rate: %.2f longs/sec%n", readsPerSecond);
    }
}
