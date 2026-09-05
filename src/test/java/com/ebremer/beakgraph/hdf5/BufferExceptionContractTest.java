package com.ebremer.beakgraph.hdf5;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.hdf5.writers.FCDWriter;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The in-memory writer buffers cannot fail to construct or close, and say so:
 * DataOutputBuffer used to declare a FileNotFoundException it never raised and
 * a {@code close() throws Exception}, which every dictionary writer had to
 * plumb through (BG-89).
 */
class BufferExceptionContractTest {

    private static List<Class<?>> thrownBy(java.lang.reflect.Executable e) {
        return List.of(e.getExceptionTypes());
    }

    @Test
    void constructionAndCloseDeclareNoCheckedExceptions() throws Exception {
        assertEquals(List.of(), thrownBy(DataOutputBuffer.class.getConstructor(Path.class)));
        assertEquals(List.of(), thrownBy(DataOutputBuffer.class.getMethod("close")));
        assertEquals(List.of(), thrownBy(FCDWriter.class.getConstructor(Path.class, int.class)));
        assertEquals(List.of(), thrownBy(FCDWriter.class.getMethod("close")));
        assertEquals(List.of(), thrownBy(MultiTypeDictionaryWriter.class.getMethod("close")));
        assertEquals(List.of(), thrownBy(BitPackedUnSignedLongBuffer.class.getConstructor(Path.class, int.class)));
    }

    @Test
    void closeIsIdempotentAndKeepsWhatWasWritten() throws Exception {
        DataOutputBuffer doubles = new DataOutputBuffer(Path.of("doubles"));
        doubles.writeDouble(1.5);
        doubles.close();
        doubles.close();
        assertEquals(1, doubles.getNumEntries());

        FCDWriter strings = new FCDWriter(Path.of("strings"), 4);
        strings.add("alpha");
        strings.add("alphabet");
        strings.close();
        strings.close();
        assertEquals(2, strings.getNumEntries());
    }
}
