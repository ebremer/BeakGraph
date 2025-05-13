package com.ebremer.beakgraph.HDTish;

import io.jhdf.api.WritableDataset;
import java.util.Map;

/**
 *
 * @author Erich Bremer
 */
public class UTIL {
    
    public static WritableDataset putAttributes( WritableDataset ds, Map<String, Object> attributes ) {
        attributes.forEach((k,v)->{
            ds.putAttribute(k, v);
        });
        return ds;
    }
    
    public static long MinBits(long x) {
        return (long) Math.ceil(Math.log(x)/Math.log(2d));
    }
    
    public static String byteArrayToBinaryString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 8);
        for (byte b : bytes) {
            for (int i = 7; i >= 0; i--) {
                int bit = (b >> i) & 1;
                sb.append(bit);
            }
        }
        return sb.toString();
    }
}
