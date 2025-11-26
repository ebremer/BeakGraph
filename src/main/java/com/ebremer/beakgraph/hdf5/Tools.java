package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;

public class Tools {
    
    public static String bytesToBinaryString(ByteBuffer bytes, int width) {
        StringBuilder sb = new StringBuilder();
        int c = 0;
        String fmt = "%"+width+"s";
        while (bytes.hasRemaining()) {
            byte b = bytes.get();
            c++;
            //"%8s"
//            IO.println(b+" ==> "+String.format(fmt, Integer.toBinaryString(b & 0xFF)));
            String binary = String.format(fmt, Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            sb.append(binary);
        }
//        IO.println(width+" Buffer Length : "+c+"   "+sb.toString());
        return sb.toString();
    }

    public static void displayDataByWidth(ContiguousDataset dataset) {
        if (dataset == null) {
            System.err.println("Error: Dataset cannot be null.");
            return;
        }
        try {
            Long xwidth = (Long) dataset.getAttribute("width").getData();
            int width = xwidth.intValue();
            if (width <= 0) {
                System.err.println("Error: 'width' attribute must be a positive integer, but was " + width + ".");
                return;
            }
            Object dataObject = dataset.getData();
            if (dataObject == null || !dataObject.getClass().isArray()) {
                System.err.println("Error: Dataset data is null or not a recognized array format.");
                return;
            }         
            String dataString = bytesToBinaryString(dataset.getBuffer(),width);
            System.out.print("Displaying data from dataset '" + dataset.getPath() + "' with width = " + width + " : ");
            for (int i = 0; i < dataString.length(); i += width) {
                int endIndex = Math.min(i + width, dataString.length());
                System.out.print(dataString.substring(i, endIndex)+" ");
            }
            System.out.println();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred while processing the dataset: " + e.getMessage());
            e.printStackTrace();
        }
    }
}