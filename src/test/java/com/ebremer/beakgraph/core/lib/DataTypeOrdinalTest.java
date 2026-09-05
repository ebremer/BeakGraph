package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.utils.UTIL;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-28: the {@code datatypes} column stores {@link DataType} ordinals, so
 * the enum order is on-disk format (SPECIFICATIONS.md §7.4: append only,
 * never reorder, never recycle). Every round-trip test writes and reads with
 * the same enum, so a reorder or a mid-table insertion would pass CI and
 * silently decode every existing store's kinds wrong. This pins the table
 * and the derived column width.
 */
class DataTypeOrdinalTest {

    @TempDir
    static Path dir;

    @Test
    void ordinalsMatchTheSpecificationTable() {
        assertEquals(14, DataType.values().length, "a new kind must be APPENDED (and §7.4 extended)");
        assertEquals(0, DataType.BNODE.ordinal());
        assertEquals(1, DataType.IRI.ordinal());
        assertEquals(2, DataType.STRING.ordinal());
        assertEquals(3, DataType.BYTE.ordinal());
        assertEquals(4, DataType.BOOLEAN.ordinal());
        assertEquals(5, DataType.SHORT.ordinal());
        assertEquals(6, DataType.INTEGER.ordinal());
        assertEquals(7, DataType.LONG.ordinal());
        assertEquals(8, DataType.FLOAT.ordinal());
        assertEquals(9, DataType.DOUBLE.ordinal());
        assertEquals(10, DataType.BIG_INTEGER.ordinal());
        assertEquals(11, DataType.BIG_DECIMAL.ordinal());
        assertEquals(12, DataType.RELATIVE_IRI.ordinal());
        assertEquals(13, DataType.TRIPLE_TERM.ordinal());
    }

    @Test
    void theDatatypesColumnIsFiveBitsWide() throws Exception {
        assertEquals(5, 1 + UTIL.MinBits(DataType.values().length), "5 bits while the enum has at most 15 values");
        // ... and that is the width every writer stamps on the column.
        File ttl = dir.resolve("dt.ttl").toFile();
        File h5 = dir.resolve("dt.ttl.h5").toFile();
        Files.write(ttl.toPath(), """
            @prefix ex: <http://ex.org/> .
            ex:s ex:p ex:o ; ex:n 1 ; ex:d 1.5 ; ex:l "x" ; ex:t <<( ex:a ex:b ex:c )>> .
            """.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        List<String> seen = new ArrayList<>();
        try (HdfFile hdf = new HdfFile(h5.toPath())) {
            collect(hdf, seen);
        }
        assertTrue(!seen.isEmpty(), "the store must hold at least one datatypes column");
    }

    private static void collect(Group group, List<String> seen) {
        for (io.jhdf.api.Node child : group.getChildren().values()) {
            if (child.isGroup()) {
                collect((Group) child, seen);
            } else if (child instanceof Dataset ds && "datatypes".equals(ds.getName())) {
                Attribute width = ds.getAttribute("width");
                assertEquals(5, scalar(width.getData()), ds.getPath() + " width");
                seen.add(ds.getPath());
            }
        }
    }

    private static long scalar(Object data) {
        if (data.getClass().isArray()) {
            return ((Number) java.lang.reflect.Array.get(data, 0)).longValue();
        }
        return ((Number) data).longValue();
    }
}
