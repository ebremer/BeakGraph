package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.store.Container;
import com.ebremer.beakgraph.store.OpenStore;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Locale;

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
     * Opens the store a URI names: a {@code file:} URI (or one with no scheme, a path) the local store, an
     * {@code http:} / {@code https:} URL the store on that server, queried
     * in place by range requests - a URL whose path ends in {@code .zarr} as
     * a Zarr store, any other as an HDF5 file. The graph reports the URL
     * without its query as its identity (a presigned URL carries its
     * signature there).
     *
     * @throws IllegalArgumentException for any other scheme, and for a URL
     *         that names a ZIP archive, which is read from a local file only
     */
    public static BeakGraph getBeakGraph(URI uri) throws IOException {
        if (uri.getScheme() == null) {
            return getBeakGraph(Path.of(uri.getPath())); // a path written as a URI reference
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("file")) {
            return getBeakGraph(Path.of(uri));
        }
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new IllegalArgumentException("BeakGraph reads file: and http(s): store URIs, got: " + uri);
        }
        Container container = Container.ofRemote(uri);
        if (container == Container.HDF5) {
            return getBeakGraph(new HTTPSeekableByteChannel(uri));
        }
        if (container == Container.ZARR_ZIP) {
            throw new IllegalArgumentException("A Zarr store in a ZIP archive is read from a local file, not over"
                    + " HTTP: serve the store as a directory (.zarr) or as an HDF5 file. Got: "
                    + HTTPSeekableByteChannel.redact(uri));
        }
        // The graph is named as a channel names one: without the query, where a presigned URL carries its signature.
        String display = HTTPSeekableByteChannel.redact(uri);
        String marker = "?query-redacted";
        URI source = URI.create(display.endsWith(marker) ? display.substring(0, display.length() - marker.length()) : display);
        HDF5Reader reader;
        try {
            reader = new HDF5Reader(OpenStore.open(uri), source); // owns (and on failure closes) the store
        } catch (UncheckedIOException e) {
            throw new IOException(e.getMessage(), e.getCause()); // "Cannot open <the URL, without its query>: ..."
        }
        try {
            return new BeakGraph(reader);
        } catch (RuntimeException | Error e) {
            try { reader.close(); } catch (Exception ignore) {}
            throw e;
        }
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
