package com.ebremer.beakgraph.store;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A static file server for the tests, on the loopback interface only: the
 * files of a directory tree, whole or by one byte range, with the strong
 * validator ({@code ETag}: size and modification time) and the answers a
 * plain web server or an object store gives - 206 with Content-Range, 200 for
 * the whole value when {@code If-Range} no longer matches, 404 for what is
 * not there, HEAD. It records every request, so that a test can say how many
 * a read cost and that none fetched a value whole.
 */
final class TreeServer implements AutoCloseable {

    private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");

    private final HttpServer server;
    private final Path root;
    /** One line a request: {@code METHOD /path?query [bytes=a-b]}. */
    final List<String> requests = new CopyOnWriteArrayList<>();

    TreeServer(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    /** The URL of {@code path} (which begins with a slash) on this server. */
    URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    /** The requests recorded so far that begin with {@code prefix}, e.g. {@code "GET "}. */
    List<String> requests(String prefix) {
        return requests.stream().filter(r -> r.startsWith(prefix)).toList();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            URI uri = exchange.getRequestURI();
            String range = exchange.getRequestHeaders().getFirst("Range");
            requests.add(method + " " + uri.getRawPath() + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery())
                    + (range == null ? "" : " " + range));
            Path file = root.resolve(uri.getPath().substring(1)).normalize();
            if (!file.startsWith(root) || !Files.isRegularFile(file)) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                long size = channel.size();
                String etag = "\"" + size + "-" + Files.getLastModifiedTime(file).toMillis() + "\"";
                exchange.getResponseHeaders().set("ETag", etag);
                exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
                if (method.equals("HEAD")) {
                    exchange.getResponseHeaders().set("Content-Length", Long.toString(size));
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                long start = 0;
                long end = size - 1;
                int status = 200;
                String ifRange = exchange.getRequestHeaders().getFirst("If-Range");
                Matcher m = range == null ? null : RANGE.matcher(range);
                if (m != null && m.matches() && (ifRange == null || ifRange.equals(etag))) {
                    start = Long.parseLong(m.group(1));
                    end = Math.min(size - 1, Long.parseLong(m.group(2)));
                    if (start >= size) {
                        exchange.getResponseHeaders().set("Content-Range", "bytes */" + size);
                        exchange.sendResponseHeaders(416, -1);
                        return;
                    }
                    status = 206;
                    exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + size);
                }
                long length = end - start + 1;
                if (length == 0) {
                    exchange.sendResponseHeaders(status, -1);
                    return;
                }
                exchange.sendResponseHeaders(status, length);
                ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, 1 << 16));
                try (OutputStream out = exchange.getResponseBody()) {
                    long at = start;
                    while (at <= end) {
                        buffer.clear().limit((int) Math.min(buffer.capacity(), end - at + 1));
                        int n = channel.read(buffer, at);
                        if (n <= 0) {
                            break;
                        }
                        out.write(buffer.array(), 0, n);
                        at += n;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
