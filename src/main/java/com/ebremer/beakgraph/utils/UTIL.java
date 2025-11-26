package com.ebremer.beakgraph.utils;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.core.Dictionary;
import io.jhdf.api.WritableDataset;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */

public class UTIL {
    
    public static String byteArrayToBinaryString(byte[] array, int length) {
        StringBuilder sb = new StringBuilder(length * 8 + (length - 1));
        for (int i = 0; i < length; i++) {
            sb.append(String.format("%8s", Integer.toBinaryString(array[i] & 0xFF))
                        .replace(' ', '0'));
            if (i < length - 1) sb.append(' ');
        }
        return sb.toString();
    }
    
    public static String toBinaryString(ByteBuffer buffer, int offset) {
        // Use duplicate so we don't modify the original buffer's position
        ByteBuffer dup = buffer.duplicate();
        StringBuilder sb = new StringBuilder((dup.limit() - dup.position()) * 9);
        while (dup.hasRemaining()) {
            int b = dup.get() & 0xFF;
            // format to 8-bit binary, pad with leading zeros
            byte[] ha = new byte[1];
            ha[0] = (byte) b;
            String s = new String(ha, StandardCharsets.UTF_8);
            int ye = offset+dup.position();
            sb.append(String.format("%d : %8s -- %s ==> [%s]", ye, Integer.toBinaryString(b), Integer.toHexString(b), s));
                      //    .replace(' ', '0'));
            if (dup.hasRemaining()) sb.append('\n');
        }
        return sb.toString();
    }

    public static byte[] getBytes(ByteBuffer buffer) {
        String cn = buffer.getClass().getName();
        if (cn.equals("java.nio.HeapByteBuffer2")) {
            buffer.position(0);
            int r = buffer.capacity();
            byte[] b = new byte[r];
            buffer.get(b);
            return b;
        }
        return buffer.array();
    }
    
    public static ByteBuffer subBuffer(ByteBuffer src, int offset, int length) {
        ByteBuffer dup = src.duplicate();
        dup.position(offset);
        dup.limit(offset + length);
        return dup.slice();
    }

    public static void skipNullTerminatedStrings(ByteBuffer buffer, int skip) {
        if (skip > 0) {
            int currentPosition = buffer.position();
            int limit = buffer.limit();
            for (int i = 0; i < skip; i++) {
                while (currentPosition < limit) {
                    if (buffer.get(currentPosition) == 0) {
                        currentPosition++;
                        break;
                    }
                    currentPosition++;
                }
            }
            buffer.position(currentPosition);
        }
    }
    
    public static String readNullTerminatedString(ByteBuffer buffer) {
        int limit = buffer.limit();
        int startPosition = buffer.position();
        int endPosition = startPosition;
        while ((endPosition < limit) && buffer.get(endPosition) != 0) {
            endPosition++;
        }
        //if (endPosition == limit && buffer.get(endPosition -1) != 0) {}
        byte[] stringBytes = new byte[endPosition - startPosition];
        buffer.position(startPosition);
        buffer.get(stringBytes);
        buffer.position(endPosition+1);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }
    
    public static WritableDataset putAttributes( WritableDataset ds, Map<String, Object> attributes ) {
        attributes.forEach((k,v)->{
            ds.putAttribute(k, v);
        });
        return ds;
    }
    
    public static int MinBits(long x) {
        if (x == 0) return 1;
        return Long.SIZE - Long.numberOfLeadingZeros(x);
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
    
    
    /**
     * Given a boundary index (RRR) and a 0-based bit position, return the 1-based dictionary ID
     * whose run contains that position. Returns 0 if none.
     * @param idx
     * @param pos
     * @return 
     */
    public static long idForPosition(RRR idx, long pos) {
        long lo = 1, hi = idx.getTotalOnes();
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long p   = idx.select(mid);
            if (p == pos) {
                return mid;
            } else if (p < pos) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi > 0 ? hi : 0;
    }

    /**
     * Retrieves all predicates (as Nodes) for a given subject ID in SPO order.
     * Requires subject and predicate indexes and a predicate dictionary reader.
     * @param subjIndex
     * @param predDict
     * @param predIndex
     * @param subjectId
     * @return 
     */
    public static List<Node> getPredicatesForSubject(
            RRR subjIndex,
            RRR predIndex,
            long subjectId,
            Dictionary predDict
    ) {
        long[] range = subjIndex.selectRange(subjectId);
        if (range == null) return Collections.emptyList();
        List<Node> preds = new ArrayList<>();
        for (long t = range[0]; t < range[1]; t++) {
            long pid = idForPosition(predIndex, t);
            if (pid > 0) preds.add(predDict.extract((int)pid));
        }
        return preds;
    }

    /**
     * Retrieves all objects for a given subject and predicate ID in SPO order.
     * @param subjIndex
     * @param predIndex
     * @param objIndex
     * @param subjectId
     * @param predicateId
     * @param objDict
     * @return 
     */
    public static List<Node> getObjectsForSubjectAndPredicate(
            RRR subjIndex,
            RRR predIndex,
            RRR objIndex,
            long subjectId,
            long predicateId,
            Dictionary objDict
    ) {
        long[] range = subjIndex.selectRange(subjectId);
        if (range == null) return Collections.emptyList();
        List<Node> objs = new ArrayList<>();
        for (long t = range[0]; t < range[1]; t++) {
            long pid = idForPosition(predIndex, t);
            if (pid != predicateId) continue;
            long oid = idForPosition(objIndex, t);
            if (oid > 0) objs.add(objDict.extract((int)oid));
        }
        return objs;
    }

    /**
     * Returns all subject IDs (1-based) in ascending order.
     * @param subjIndex
     * @return 
     */
    public static List<Long> getSubjectIds(RRR subjIndex) {
        List<Long> ids = new ArrayList<>();
        for (long id = 1; id <= subjIndex.getTotalOnes(); id++) {
            ids.add(id);
        }
        return ids;
    }

    /**
     * Returns the [subject, predicate, object] Nodes for a given triple-position t.
     * @param t
     * @param objDict
     * @param predIndex
     * @param subjIndex
     * @param subjDict
     * @param predDict
     * @param objIndex
     * @return 
     */
    public static Node[] getTripleAtPosition(
            long t,
            RRR subjIndex,
            RRR predIndex,
            RRR objIndex,
            Dictionary subjDict,
            Dictionary predDict,
            Dictionary objDict
    ) {
        long sid = idForPosition(subjIndex, t);
        long pid = idForPosition(predIndex, t);
        long oid = idForPosition(objIndex, t);
        return new Node[]{
            subjDict.extract((int)sid),
            predDict.extract((int)pid),
            objDict.extract ((int)oid)
        };
    }

    /**
     * Returns all zero-bit positions (0-based) in ascending order.
     * @param idx
     * @return 
     */
    public static List<Long> getZeroPositions(RRR idx) {
        long totalZeros = idx.getSizeInBits() - idx.getTotalOnes();
        List<Long> zeros = new ArrayList<>();
        for (long j = 1; j <= totalZeros; j++) {
            long pos = idx.select0(j);
            if (pos >= 0) zeros.add(pos);
        }
        return zeros;
    }

    /**
     * Builds an RRR index from a BitSet of length numBits.
     * @param bits
     * @param numBits
     * @param superblockSize
     * @param blockSize
     * @return 
     */
    public static RRR buildFromBitSet(
            BitSet bits,
            int numBits,
            int superblockSize,
            int blockSize
    ) {
        int numSuperblocks = (numBits + superblockSize - 1) / superblockSize;
        BitPackedUnSignedLongBuffer sbBuf = new BitPackedUnSignedLongBuffer(null, null, 0, 64);
        BitPackedUnSignedLongBuffer blBuf = new BitPackedUnSignedLongBuffer(null, null, 0, 64);
        long sbCount = 0;
        for (int s = 0; s < numSuperblocks; s++) {
            sbBuf.writeLong(sbCount);
            int sbStart = s * superblockSize;
            int sbEnd = Math.min(numBits, sbStart + superblockSize);
            long rel = 0;
            int blocksPerSB = superblockSize / blockSize;
            blBuf.writeLong(0);
            long superPop = 0;
            for (int b = 0; b < blocksPerSB; b++) {
                int bStart = sbStart + b * blockSize;
                int bEnd = Math.min(sbEnd, bStart + blockSize);
                long blockPop = 0;
                for (int i = bStart; i < bEnd; i++) if (bits.get(i)) blockPop++;
                rel += blockPop;
                blBuf.writeLong(rel);
                superPop += blockPop;
            }
            sbCount += superPop;
        }
        sbBuf.writeLong(sbCount);
        sbBuf.prepareForReading();
        blBuf.prepareForReading();
        return new RRR(sbBuf, blBuf, superblockSize, blockSize);
    }
}
