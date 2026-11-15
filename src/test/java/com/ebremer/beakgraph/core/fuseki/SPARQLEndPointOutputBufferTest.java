package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.RetainableByteBuffer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;

/**
 * The size of the buffer the endpoint's responses are gathered in. Fuseki sets
 * 5 MiB, which Jetty's buffer pool does not keep: every response then had a
 * 5 MiB direct buffer allocated, zero-filled and freed for it - most of what a
 * small query cost at the endpoint. These tests hold the reason for the size
 * BeakGraph sets instead: it is one the pool keeps.
 */
class SPARQLEndPointOutputBufferTest {

    private static final int FUSEKI = 5 * 1024 * 1024;

    /** A server configured as Fuseki configures its own: one connector, a 5 MiB output buffer. */
    private static Server fusekiLike(HttpConfiguration configuration) {
        configuration.setOutputBufferSize(FUSEKI);
        Server server = new Server();
        server.addConnector(new ServerConnector(server, new HttpConnectionFactory(configuration)));
        return server;
    }

    @Test
    void everyConnectorGetsTheDefaultSize() {
        HttpConfiguration configuration = new HttpConfiguration();
        Server server = fusekiLike(configuration);
        assertEquals(FUSEKI, configuration.getOutputBufferSize());
        if (System.getProperty("beakgraph.http.output.buffer") == null) {
            assertEquals(SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER, SPARQLEndPoint.sizeOutputBuffers(server));
            assertEquals(SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER, configuration.getOutputBufferSize());
        }
    }

    @Test
    void aConfiguredSizeIsNeverUnderWhatAResponseHoldsBack() {
        assertEquals(64 * 1024, SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER);
        assertEquals(1 << 20, SPARQLEndPoint.outputBufferSize(1 << 20), "a larger buffer is taken as asked");
        assertEquals(32 * 1024, SPARQLEndPoint.outputBufferSize(32 * 1024));
        // Below the threshold the container would commit a response by itself before
        // BGSparqlService stops holding it back, and an early error would lose its status.
        assertEquals(BGSparqlService.COMMIT_THRESHOLD, SPARQLEndPoint.outputBufferSize(4096));
        assertEquals(BGSparqlService.COMMIT_THRESHOLD, SPARQLEndPoint.outputBufferSize(0));
        assertEquals(BGSparqlService.COMMIT_THRESHOLD, SPARQLEndPoint.outputBufferSize(-1));
        assertEquals(Integer.MAX_VALUE, SPARQLEndPoint.outputBufferSize(Long.MAX_VALUE));
        assertTrue(SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER >= BGSparqlService.COMMIT_THRESHOLD);
    }

    /**
     * Why 64 KiB: a buffer of the default size, released, is the one the pool
     * hands out next; one of Fuseki's size is made anew each time. A Jetty
     * whose pool kept other sizes would fail here, not slow the endpoint down
     * unnoticed.
     */
    @Test
    void theDefaultSizeIsOneJettysPoolKeeps() throws Exception {
        Server server = fusekiLike(new HttpConfiguration());
        server.start();
        try {
            ByteBufferPool pool = server.getByteBufferPool();
            assertSame(acquireAndRelease(pool, SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER), acquireAndRelease(pool, SPARQLEndPoint.DEFAULT_OUTPUT_BUFFER),
                    "a 64 KiB buffer is kept and handed out again");
            assertNotSame(acquireAndRelease(pool, FUSEKI), acquireAndRelease(pool, FUSEKI),
                    "a 5 MiB buffer is allocated for each response that asks for one");
        } finally {
            server.stop();
        }
    }

    private static ByteBuffer acquireAndRelease(ByteBufferPool pool, int size) {
        RetainableByteBuffer buffer = pool.acquire(size, true);
        ByteBuffer bytes = buffer.getByteBuffer();
        assertTrue(bytes.capacity() >= size);
        buffer.release();
        return bytes;
    }
}
