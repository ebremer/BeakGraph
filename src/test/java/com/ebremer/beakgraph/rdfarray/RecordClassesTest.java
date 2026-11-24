package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.SequenceArrays;
import com.ebremer.beakgraph.store.OpenStore;
import com.ebremer.beakgraph.store.StoreGroup;
import com.ebremer.beakgraph.store.TypedArray;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Record classes (RDFARRY.md §3.5a, SPECIFICATIONS.md §7.6c) in both
 * containers: a typed column per leaf position (strings, IRIs, language
 * strings with their codes, dateTimes and dates as CF counts, numeric blocks),
 * masks for slots, {@code starts} for lists, and the class numbers in
 * {@code sequenceArrays} after the tensor and ragged classes.
 */
class RecordClassesTest {

    @TempDir
    Path dir;

    static final String DATA = """
        @prefix : <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :obs1 :component ( ( "8480-6" "systolic" ( 120 "mm[Hg]" ) ) ( "8462-4" "diastolic" ( 80 "mm[Hg]" ) ) ) .
        :obs2 :component ( ( "8480-6" "systolic" ( 150 "mm[Hg]" ) ) ( "8462-4" "diastolic" ( 95 "mm[Hg]" ) ) ) .
        :obs3 :component ( ( "8480-6" "systolic" ( "high" "mm[Hg]" ) ) ) .
        :r1 :reading ( "2024-01-01T10:00:00Z"^^xsd:dateTime 5.5E0 ) .
        :r2 :reading ( "1969-12-31T23:59:59.25Z"^^xsd:dateTime -1.0E0 ) .
        :z1 :local ( "2024-01-01T10:00:00.123456+05:00"^^xsd:dateTime "2024-02-29"^^xsd:date ) .
        :n1 :names ( "chat"@fr "cat"@en ) .
        :n2 :names ( "dog"@en undef ) .
        :l1 :link ( ( :a "label a" ) ( :b "label b" ) ) .
        :e1 :emb ( "first" ( 1.0E0 2.0E0 3.0E0 ) ) .
        :e2 :emb ( "second" ( 4.0E0 undef 6.0E0 ) ) .
        :v1 :vector ( 1 2 3 ) .
        :u1 :bnode ( "a" _:b ) .
        :u2 :decimal ( "a" 0.5 ) .
        :u3 :untyped ( ( "a" undef ) ( "b" undef ) ) .
        :u4 :padded ( "2024-01-01T10:00:00.50Z"^^xsd:dateTime 1.0E0 ) .
        :u5 :zero ( "2024-01-01T10:00:00+00:00"^^xsd:dateTime 1.0E0 ) .
        """;

    /** In signature order: tuples before lists ('(' before 'l'), '+' before 'Z'. */
    static final List<String> SIGNATURES = List.of(
            "(dt+05:00,date)", "(dtZ,f64)", "(str,f64[3])", "list((iri,str))",
            "list((str,str,(i64,str)))", "list((str,str,(str,str)))", "list(lang)");

    private File build(String name, SequenceArrays.Options options) throws Exception {
        Path src = dir.resolve("records.ttl");
        Files.writeString(src, DATA);
        File out = dir.resolve(name).toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).setArrays(options).build().write();
        return out;
    }

    private static SequenceArrays.Options everyClass(boolean compress) {
        return new SequenceArrays.Options(1, 2, compress);
    }

    @ParameterizedTest
    @ValueSource(strings = {"records.h5", "records.zarr", "compressed.h5", "compressed.zarr"})
    void recordClassesHoldOneColumnPerPosition(String name) throws Exception {
        File store = build(name, everyClass(name.startsWith("compressed")));
        boolean zarr = name.endsWith(".zarr");
        try (OpenStore open = OpenStore.open(store.toPath()); BeakGraph bg = BG.getBeakGraph(store)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) bg.getReader().getDictionary();
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            // ( 1 2 3 ) is section 3.5's; the five :u* sequences pack nowhere.
            assertEquals(List.of("i64_3", "rec_1", "rec_2", "rec_3", "rec_4", "rec_5", "rec_6", "rec_7"), arrays.groupNames());
            for (int i = 0; i < SIGNATURES.size(); i++) {
                StoreGroup g = arrays.group("rec_" + (i + 1));
                assertEquals("record", g.stringAttribute("kind"));
                assertEquals(SIGNATURES.get(i), g.stringAttribute("signature"));
            }

            // Appendix A.2: the FHIR components, one item per component, rows in id order (120 < 150).
            StoreGroup fhir = arrays.group("rec_5");
            assertArrayEquals(new long[]{0, 2, 4}, (long[]) fhir.typedArray("starts").values());
            assertArrayEquals(new String[]{"8480-6", "8462-4", "8480-6", "8462-4"}, strings(fhir, "f1"));
            assertArrayEquals(new String[]{"systolic", "diastolic", "systolic", "diastolic"}, strings(fhir, "f2"));
            TypedArray value = fhir.typedArray("f3_1");
            assertEquals(TypedArray.Type.I64, value.type());
            assertArrayEquals(new long[]{120, 80, 150, 95}, (long[]) value.values());
            assertEquals("http://www.w3.org/2001/XMLSchema#integer", value.attributes().get("datatype"));
            assertEquals("i64", value.attributes().get("kind"));
            assertArrayEquals(new String[]{"mm[Hg]", "mm[Hg]", "mm[Hg]", "mm[Hg]"}, strings(fhir, "f3_2"));
            assertNull(fhir.typedArray("f1_mask"), "no slot, no mask");
            if (zarr) {
                assertEquals(List.of("item"), fhir.typedArray("f1").dimensionNames());
            }
            long[] fhirSeq = (long[]) fhir.typedArray("seq").values();
            assertEquals(2, fhirSeq.length);
            assertTrue(fhirSeq[0] < fhirSeq[1]);
            assertArrayEquals(new long[]{0, 1}, (long[]) arrays.group("rec_6").typedArray("starts").values(), ":obs3, alone");

            // (dtZ,f64): UTC instants in microseconds; f1 and f2 pair row by row.
            StoreGroup readings = arrays.group("rec_2");
            TypedArray when = readings.typedArray("f1");
            assertEquals(TypedArray.Type.I64, when.type());
            assertEquals(Map.of("calendar", "proleptic_gregorian", "datatype", "http://www.w3.org/2001/XMLSchema#dateTime",
                    "kind", "dtZ", "timezone", "Z", "units", "microseconds since 1970-01-01T00:00:00"), when.attributes());
            long[] t = (long[]) when.values();
            double[] v = (double[]) readings.typedArray("f2").values();
            for (int r = 0; r < 2; r++) {
                assertEquals(t[r] == 1_704_103_200_000_000L ? 5.5 : -1.0, v[r]);
                assertTrue(t[r] == 1_704_103_200_000_000L || t[r] == -750_000L, Long.toString(t[r]));
            }
            if (zarr) {
                assertEquals(List.of("seq"), when.dimensionNames(), "a tuple class has a row per sequence");
            }

            // (dt+05:00,date): the zoned value is its UTC instant, the date int32 days.
            StoreGroup local = arrays.group("rec_1");
            assertArrayEquals(new long[]{1_704_085_200_123_456L}, (long[]) local.typedArray("f1").values());
            assertEquals("+05:00", local.typedArray("f1").attributes().get("timezone"));
            TypedArray day = local.typedArray("f2");
            assertEquals(TypedArray.Type.I32, day.type());
            assertArrayEquals(new int[]{19_782}, (int[]) day.values());
            assertEquals("days since 1970-01-01", day.attributes().get("units"));

            // (str,f64[3]): the block is one column of [rows, 3], its slot masked.
            StoreGroup emb = arrays.group("rec_3");
            assertArrayEquals(new String[]{"first", "second"}, strings(emb, "f1"));
            TypedArray block = emb.typedArray("f2");
            assertArrayEquals(new long[]{2, 3}, block.shape());
            assertArrayEquals(new double[]{1, 2, 3, 4, 0, 6}, (double[]) block.values());
            assertArrayEquals(new boolean[]{false, false, false, false, true, false}, (boolean[]) emb.typedArray("f2_mask").values());
            if (zarr) {
                assertEquals(List.of("seq", "d1"), block.dimensionNames());
            }

            // list((iri,str)): the IRIs in their stored form.
            StoreGroup link = arrays.group("rec_4");
            assertArrayEquals(new String[]{"http://ex.org/a", "http://ex.org/b"}, strings(link, "f1"));
            assertEquals("iri", link.typedArray("f1").attributes().get("kind"));
            assertNull(link.typedArray("f1").attributes().get("datatype"), "an IRI has no datatype");

            // list(lang): the lexical forms, the tags as codes into langs, a slot masked.
            StoreGroup names = arrays.group("rec_7");
            TypedArray f = names.typedArray("f");
            assertEquals("en fr", f.attributes().get("langs"));
            String[] lex = (String[]) f.values();
            int[] codes = (int[]) names.typedArray("f_lang").values();
            boolean[] slot = (boolean[]) names.typedArray("f_mask").values();
            assertEquals(TypedArray.Type.U16, names.typedArray("f_lang").type());
            long[] nameSeq = (long[]) names.typedArray("seq").values();
            long[] starts = (long[]) names.typedArray("starts").values();
            for (int r = 0; r < 2; r++) {
                List<Node> el = dict.getObjects().extract(nameSeq[r]).getSeq();
                for (int j = 0; j < el.size(); j++) {
                    int at = (int) starts[r] + j;
                    if (el.get(j).isNodeUndef()) {
                        assertTrue(slot[at]);
                        assertEquals("", lex[at]);
                    } else {
                        assertEquals(el.get(j).getLiteralLexicalForm(), lex[at]);
                        assertEquals(el.get(j).getLiteralLanguage(), List.of("en", "fr").get(codes[at]));
                    }
                }
            }

            // sequenceArrays: i64_3 is class 1, rec_<n> class n + 1, the rest 0.
            long[] classes = dict.sequenceArrayClasses();
            long first = dict.firstSequenceObjectId();
            for (int c = 0; c < SIGNATURES.size(); c++) {
                for (long id : (long[]) arrays.group("rec_" + (c + 1)).typedArray("seq").values()) {
                    assertEquals(c + 2, classes[(int) (id - first)], "rec_" + (c + 1));
                }
            }
            long packed = java.util.Arrays.stream(classes).filter(x -> x != 0).count();
            assertEquals(1 + 1 + 2 + 2 + 1 + 2 + 1 + 2, packed, "i64_3, then rec_1 to rec_7's rows");
        }
    }

    private static String[] strings(StoreGroup g, String column) {
        TypedArray a = g.typedArray(column);
        assertEquals(TypedArray.Type.STR, a.type(), column);
        return (String[]) a.values();
    }

    @Test
    void minRowsPrunesRareSignatures() throws Exception {
        File store = build("pruned.h5", new SequenceArrays.Options(2, 2, false));
        try (OpenStore open = OpenStore.open(store.toPath())) {
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            // Only the signatures two sequences share; ( 1 2 3 ) is alone in i64_r.
            assertEquals(List.of("i64_r", "rec_1", "rec_2", "rec_3", "rec_4"), arrays.groupNames());
            assertEquals(List.of("(dtZ,f64)", "(str,f64[3])", "list((str,str,(i64,str)))", "list(lang)"),
                    List.of(1, 2, 3, 4).stream().map(n -> arrays.group("rec_" + n).stringAttribute("signature")).toList());
        }
    }

    @Test
    void theTimeUnitIsTheStores() throws Exception {
        File store = build("ms.zarr", new SequenceArrays.Options(1, 2, false, SequenceArrays.TimeUnit.MS));
        try (OpenStore open = OpenStore.open(store.toPath())) {
            StoreGroup arrays = open.root().group(".BG").group("arrays");
            // .123456 is finer than milliseconds, so :z1 no longer packs: rec_1 is (dtZ,f64).
            assertEquals("(dtZ,f64)", arrays.group("rec_1").stringAttribute("signature"));
            TypedArray when = arrays.group("rec_1").typedArray("f1");
            assertEquals("milliseconds since 1970-01-01T00:00:00", when.attributes().get("units"));
            long[] t = (long[]) when.values();
            assertTrue((t[0] == -750L && t[1] == 1_704_103_200_000L) || (t[1] == -750L && t[0] == 1_704_103_200_000L));
        }
    }

    @Test
    void theInMemoryEnginesWriteTheSameBytes() throws Exception {
        Path src = dir.resolve("engines.ttl");
        Files.writeString(src, DATA);
        SequenceArrays.Options o = everyClass(false);
        for (String ext : List.of(".h5", ".zarr.zip")) {
            File m0 = dir.resolve("m0" + ext).toFile();
            File m2 = dir.resolve("m2" + ext).toFile();
            File m3 = dir.resolve("m3" + ext).toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(m0).setArrays(o).build().write();
            com.ebremer.beakgraph.hdf5.writers.parallel.ParallelHDF5Writer.Builder().setSource(src.toFile())
                    .setDestination(m2).setArrays(o).setCores(3).build().write();
            com.ebremer.beakgraph.hdf5.writers.ultra.UltraHDF5Writer.Builder().setSource(src.toFile())
                    .setDestination(m3).setArrays(o).setCores(3).build().write();
            byte[] expected = Files.readAllBytes(m0.toPath());
            assertArrayEquals(expected, Files.readAllBytes(m2.toPath()), "method 2, " + ext);
            assertArrayEquals(expected, Files.readAllBytes(m3.toPath()), "method 3, " + ext);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"verify.h5", "verify.zarr", "verifyc.h5", "verifyc.zarr"})
    void verifyDeepChecksEveryRecordRow(String name) throws Exception {
        File store = build(name, everyClass(name.startsWith("verifyc")));
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int exit = new com.ebremer.beakgraph.cmdline.VerifyCommand(store, true, new java.io.PrintStream(out, true)).run();
        assertEquals(0, exit, out.toString());
    }

    @Test
    void verifyDeepNamesATamperedString() throws Exception {
        File store = build("tampered.zarr", everyClass(false));
        // rec_5's f2 is one raw vlen-utf8 chunk: "systolic" becomes "Systolic".
        Path chunk = store.toPath().resolve(".BG/arrays/rec_5/f2/c/0");
        String bytes = new String(Files.readAllBytes(chunk), StandardCharsets.ISO_8859_1);
        int at = bytes.indexOf("systolic");
        assertTrue(at > 0, "the chunk holds the strings");
        Files.write(chunk, (bytes.substring(0, at) + "S" + bytes.substring(at + 1)).getBytes(StandardCharsets.ISO_8859_1));
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int exit = new com.ebremer.beakgraph.cmdline.VerifyCommand(store, true, new java.io.PrintStream(out, true)).run();
        assertEquals(2, exit, out.toString());
        assertTrue(out.toString().contains("arrays/rec_5: row 0") && out.toString().contains("differs at f2[0]"), out.toString());
    }

    /**
     * Opt-in, as {@link SequenceArraysTest#pythonReadsTheArrays}: the FHIR
     * components as a pandas table, and xarray decoding a dateTime column to
     * {@code datetime64}. Runs where a Python with numpy, pandas, h5py, zarr
     * and xarray is on the PATH (or {@code -Dbeakgraph.python}).
     */
    @Test
    void pythonReadsTheRecords() throws Exception {
        String python = System.getProperty("beakgraph.python", "python");
        org.junit.jupiter.api.Assumptions.assumeTrue(runs(python, "-c", "import numpy, pandas, h5py, zarr, xarray"),
                "no Python with numpy, pandas, h5py, zarr and xarray (set -Dbeakgraph.python)");
        File h5 = build("py.h5", everyClass(false));
        File zarr = build("py.zarr", everyClass(true));
        Path script = dir.resolve("records.py");
        Files.writeString(script, """
            import sys, numpy as np, pandas as pd, h5py, zarr, xarray as xr
            c = zarr.open_group(sys.argv[2], mode="r")[".BG/arrays/rec_5"]
            items = pd.DataFrame({k: c[k][:] for k in ("f1", "f2", "f3_1", "f3_2")})
            items["seq"] = np.repeat(c["seq"][:], np.diff(c["starts"][:]))
            assert list(items["f3_1"]) == [120, 80, 150, 95], items
            assert list(items["f2"]) == ["systolic", "diastolic", "systolic", "diastolic"], items
            with h5py.File(sys.argv[1]) as f:
                g = f["/.BG/arrays/rec_5"]
                assert [s for s in g["f1"].asstr()[()]] == list(items["f1"])
                assert f["/.BG/arrays/rec_1/f2"].dtype == np.int32
            ds = xr.open_zarr(sys.argv[2], group=".BG/arrays/rec_2", consolidated=False)
            assert np.issubdtype(ds["f1"].dtype, np.datetime64), ds["f1"].dtype
            assert ds.sizes["seq"] == 2
            print("ok")
            """);
        assertTrue(runs(python, script.toString(), h5.toString(), zarr.toString()), "the Python reads failed");
    }

    private static boolean runs(String... command) throws Exception {
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes());
            boolean ok = p.waitFor() == 0;
            if (!ok) {
                System.err.println(String.join(" ", command) + ":\n" + out);
            }
            return ok;
        } catch (java.io.IOException noPython) {
            return false;
        }
    }
}
