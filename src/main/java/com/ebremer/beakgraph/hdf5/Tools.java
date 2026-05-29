package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tools {

    private static final Logger logger = LoggerFactory.getLogger(Tools.class);

    public static String bytesToBinaryString(ByteBuffer bytes, int width) {
        StringBuilder sb = new StringBuilder();
        String fmt = "%"+width+"s";
        while (bytes.hasRemaining()) {
            byte b = bytes.get();
            String binary = String.format(fmt, Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            sb.append(binary);
        }
        return sb.toString();
    }

    public static void displayDataByWidth(ContiguousDataset dataset) {
        if (dataset == null) {
            logger.error("Dataset cannot be null.");
            return;
        }
        try {
            Long xwidth = (Long) dataset.getAttribute("width").getData();
            int width = xwidth.intValue();
            if (width <= 0) {
                logger.error("'width' attribute must be a positive integer, but was {}.", width);
                return;
            }
            Object dataObject = dataset.getData();
            if (dataObject == null || !dataObject.getClass().isArray()) {
                logger.error("Dataset data is null or not a recognized array format.");
                return;
            }
            String dataString = bytesToBinaryString(dataset.getBuffer(),width);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dataString.length(); i += width) {
                int endIndex = Math.min(i + width, dataString.length());
                sb.append(dataString, i, endIndex).append(' ');
            }
            logger.debug("Displaying data from dataset '{}' with width = {} : {}",
                    dataset.getPath(), width, sb.toString());
        } catch (Exception e) {
            logger.error("An unexpected error occurred while processing the dataset", e);
        }
    }
}