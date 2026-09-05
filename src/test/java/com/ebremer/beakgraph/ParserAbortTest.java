package com.ebremer.beakgraph;

import org.junit.jupiter.api.parallel.Isolated;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.ebremer.beakgraph.utils.RdfSources;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-100: Jena's AsyncParser parses on its own thread and hands chunks over
 * a bounded queue. When the ingest failed part-way (a guard on an early
 * quad), the quad stream was never closed, so the parser thread stayed
 * parked forever on the full queue with a queue's worth of parsed quads -
 * one leaked daemon thread per failed large file in a batch run. Every
 * engine's ingest loop now closes the stream, which aborts and joins the
 * thread. The queue is shrunk here so a small source reproduces the shape.
 */
@Timeout(120)
// Mutates JVM-global state (system properties / ARQ modes / a shared server):
// never interleave with other classes should parallel execution be enabled (BG-189).
@Isolated
class ParserAbortTest {

    @TempDir
    static Path dir;
    static String oldChunk;
    static String oldQueue;

    @BeforeAll
    static void smallQueue() {
        oldChunk = System.setProperty(RdfSources.CHUNK_PROPERTY, "10");
        oldQueue = System.setProperty(RdfSources.QUEUE_PROPERTY, "2");
    }

    @AfterAll
    static void restore() {
        if (oldChunk == null) System.clearProperty(RdfSources.CHUNK_PROPERTY);
        else System.setProperty(RdfSources.CHUNK_PROPERTY, oldChunk);
        if (oldQueue == null) System.clearProperty(RdfSources.QUEUE_PROPERTY);
        else System.setProperty(RdfSources.QUEUE_PROPERTY, oldQueue);
    }

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    private static List<Thread> parserThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && "AsyncParser".equals(t.getName())).toList();
    }

    private static void awaitNoParserThread() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!parserThreads().isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        List<Thread> left = parserThreads();
        assertTrue(left.isEmpty(), "parser threads still alive after the failed build: " + left);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void aFailedIngestAbortsTheParserThread(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        awaitNoParserThread(); // a clean baseline
        StringBuilder ttl = new StringBuilder("""
            @prefix : <http://ex.org/> .
            @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
            :first :list "[_:b1]"^^cdt:List .
            """);
        for (int i = 0; i < 5000; i++) {
            ttl.append(":s").append(i).append(" :p :o").append(i).append(" .\n");
        }
        Path src = dir.resolve(engine.name() + ".ttl");
        Files.writeString(src, ttl.toString());
        File dest = dir.resolve(engine.name() + ".h5").toFile();
        assertThrows(Exception.class, () -> engine.buildStore(src.toFile(), dest));
        assertFalse(dest.exists());
        awaitNoParserThread();
    }
}
