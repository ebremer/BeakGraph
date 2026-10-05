package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-240: the first dictionary search used to build the sampled tier index -
 * one full front-coded block decode for every 1024th entry - which over an
 * HTTP range channel touched essentially every block of the section: the
 * whole dictionary was downloaded to answer one IRI lookup. A remote section
 * now skips the tier and binary-searches directly (O(log n) probes), so a
 * single lookup on a large dictionary fetches a small, bounded number of
 * range blocks. The fixture is big enough that the tier would exist
 * (> 1024 entities) and the download would be visible.
 */
class RemoteDictionaryLookupTest {

    private static final int ENTITIES = 200_000;
    private static final int BLOCK = 128 * 1024;   // the channel default

    @TempDir
    static Path dir;
    static byte[] h5Bytes;
    static File h5;

    /**
     * Long IRIs whose variation comes FIRST, so front-coding shares almost
     * nothing and the entities section is ~200 bytes per entry (~40 MB, some
     * 300 range blocks) - far more blocks than a binary search probes.
     */
    static String subject(int i) {
        return "http://ex.org/" + String.format(Locale.ROOT, "%08x", (i * 2654435761L) & 0xffffffffL)
                + "-" + "x".repeat(180) + "-" + i;
    }

    @BeforeAll
    static void build() throws Exception {
        StringBuilder sb = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (int i = 0; i < ENTITIES; i++) {
            sb.append("<").append(subject(i)).append("> ex:name \"name of subject ").append(i).append("\" .\n");
        }
        File ttl = dir.resolve("big.ttl").toFile();
        Files.writeString(ttl.toPath(), sb.toString(), StandardCharsets.UTF_8);
        h5 = dir.resolve("big.h5").toFile();
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        h5Bytes = Files.readAllBytes(h5.toPath());
    }

    private static List<String> names(Dataset ds, String subject) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "SELECT ?n WHERE { <" + subject + "> <http://ex.org/name> ?n }")).build()) {
            ResultSet rs = qe.execSelect();
            java.util.List<String> out = new java.util.ArrayList<>();
            while (rs.hasNext()) out.add(rs.next().getLiteral("n").getString());
            return out;
        }
    }

    @Test
    void oneLookupFetchesAFewBlocksNotTheSection() throws Exception {
        try (HttpChannelReadTest.RangeServer server = new HttpChannelReadTest.RangeServer(h5Bytes)) {
            URI uri = server.uri("/big.h5");
            HTTPSeekableByteChannel channel = new HTTPSeekableByteChannel(uri);
            try (BeakGraph remote = BG.getBeakGraph(channel)) {
                long opened = channel.getBytesFetched();
                assertEquals(List.of("name of subject 150000"), names(remote.getDataset(), subject(150000)));
                long lookup = channel.getBytesFetched() - opened;
                // The entities section is ~40 MB of text (~300 range blocks); a
                // binary search touches ~18 of them plus a few offsets/datatype
                // blocks. The sampled tier (one extract per 1024 entries) read
                // every block of the section before answering.
                assertTrue(lookup < 40L * BLOCK,
                        "one lookup fetched " + lookup + " bytes (" + (lookup / BLOCK) + " blocks of " + BLOCK + "); file is " + h5Bytes.length);
                // A second lookup on a different term is bounded the same way.
                long before = channel.getBytesFetched();
                assertEquals(List.of("name of subject 42"), names(remote.getDataset(), subject(42)));
                assertTrue(channel.getBytesFetched() - before < 40L * BLOCK);
                // A miss costs no more than a hit.
                before = channel.getBytesFetched();
                assertEquals(List.of(), names(remote.getDataset(), "http://ex.org/nowhere"));
                assertTrue(channel.getBytesFetched() - before < 40L * BLOCK);
            }
        }
    }

    @Test
    void localReaderAnswersTheSame() throws Exception {
        try (BeakGraph local = BG.getBeakGraph(h5)) {
            assertEquals(List.of("name of subject 150000"), names(local.getDataset(), subject(150000)));
            assertEquals(List.of(), names(local.getDataset(), "http://ex.org/nowhere"));
        }
    }
}
