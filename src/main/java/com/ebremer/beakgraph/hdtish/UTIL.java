package com.ebremer.beakgraph.hdtish;

/**
 *
 * @author Erich Bremer
 */
public class UTIL {
    
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
