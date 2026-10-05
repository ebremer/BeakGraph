package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.io.ByteBufferBytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * BG-26: the VByte codec had no direct tests. Round trips across the value
 * range, the nine-byte maximum the encoder emits, and rejection of a longer
 * sequence - a tenth byte used to have its low bits shifted out of the long
 * silently before the guard fired on the eleventh.
 */
class VByteTest {

    private static byte[] encode(long... values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (long v : values) {
            VByte.encode(out, v);
        }
        return out.toByteArray();
    }

    @Test
    void roundTripsAcrossTheValueRange() throws IOException {
        long[] values = {0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE, 1L << 35, Long.MAX_VALUE};
        byte[] bytes = encode(values);
        ByteBufferBytes region = new ByteBufferBytes(ByteBuffer.wrap(bytes));
        long pos = 0;
        for (long v : values) {
            VByte.DecodeResult r = VByte.decodeAt(region, pos);
            assertEquals(v, r.value, "value at " + pos);
            pos = r.nextOffset;
        }
        assertEquals(bytes.length, pos, "every byte consumed");
    }

    @Test
    void theEncoderNeverEmitsMoreThanNineBytes() throws IOException {
        assertEquals(VByte.MAX_BYTES, encode(Long.MAX_VALUE).length);
        assertEquals(1, encode(0).length);
        assertEquals(1, encode(127).length);
        assertEquals(2, encode(128).length);
        assertThrows(IllegalArgumentException.class, () -> VByte.encode(new ByteArrayOutputStream(), -1));
    }

    @Test
    void aTenByteSequenceIsRejectedNotSilentlyTruncated() {
        // Nine continuation bytes (no stop bit) then a terminated tenth byte
        // whose low bits would land at shift 63 - i.e. mostly shifted out.
        byte[] ten = {0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xC3};
        ByteBufferBytes region = new ByteBufferBytes(ByteBuffer.wrap(ten));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> VByte.decodeAt(region, 0));
        assertTrue(ex.getMessage().contains("too long"), ex.getMessage());
        assertTrue(ex.getMessage().contains("offset 0"), ex.getMessage());
        // The longest legal sequence at the same position still decodes.
        byte[] nine = {0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, (byte) 0xFF};
        assertEquals(Long.MAX_VALUE, VByte.decodeAt(new ByteBufferBytes(ByteBuffer.wrap(nine)), 0).value);
    }
}
