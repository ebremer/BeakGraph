package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public class BG {
    
    public static HDF5Writer.Builder getBGWriterBuilder() { 
        return HDF5Writer.Builder();
    }
    
    public static BeakGraph getBeakGraph(File file) throws IOException {
        HDF5Reader reader = new HDF5Reader(file);
        try {
            return new BeakGraph(reader);
        } catch (RuntimeException | Error e) {
            // The reader pins the mapped file; if BeakGraph construction fails it
            // must be released here or nobody ever can.
            try { reader.close(); } catch (Exception ignore) {}
            throw e;
        }
    }

    public static BeakGraph getBeakGraph(Path path) throws IOException {
        return getBeakGraph(path.toFile());
    }

    /**
     * Opens a BeakGraph over any {@link SeekableByteChannel} - e.g.
     * {@code new HTTPSeekableByteChannel(uri)} to query a remote file in
     * place via HTTP range requests. The graph takes ownership of the
     * channel: closing the graph closes it, and it is also released if
     * opening fails.
     */
    public static BeakGraph getBeakGraph(SeekableByteChannel sbc) throws IOException {
        // The redacted form: a presigned URL's signature must not become the
        // graph's identity nor appear in the reader's messages (BG-225).
        URI source = (sbc instanceof HTTPSeekableByteChannel http)
                ? http.getDisplayURI()
                : URI.create("bg:/channel");
        return getBeakGraph(sbc, source);
    }

    /**
     * As {@link #getBeakGraph(SeekableByteChannel)}, with an explicit URI to
     * report as the graph's identity ({@code BeakGraph.getURI()}).
     * @param sbc
     * @param source
     * @return 
     * @throws java.io.IOException
     */
    public static BeakGraph getBeakGraph(SeekableByteChannel sbc, URI source) throws IOException {
        HDF5Reader reader = new HDF5Reader(sbc, source); // owns (and on failure closes) the channel
        try {
            return new BeakGraph(reader);
        } catch (RuntimeException | Error e) {
            try { reader.close(); } catch (Exception ignore) {}
            throw e;
        }
    }
}
