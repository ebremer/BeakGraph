# Vendored: io.airlift.compress.v3.zstdFFM

Copied from the `zstdFFM` branch of https://github.com/ebremer/aircompressor
(`src/main/java/io/airlift/compress/v3/zstdFFM`) on 2026-07-02, package name
preserved. That branch was forked from upstream airlift/aircompressor commit
`73002e3f4e849f10ddbb3c7ba345ccaa887dd3b8`.

Fork commit at the time of vendoring: NOT RECORDED (fill in from the fork's
history before the next re-vendor; until then the divergence list below is the
only reliable description of how this copy relates to the fork).

This is the FFM (Foreign Function & Memory API) port of aircompressor's pure-Java
Zstd codec — it replaces the sun.misc.Unsafe-based implementation in
`io.airlift.compress.v3.zstd`. It is vendored here until the upstream
aircompressor project publishes a release containing this package (upstream 3.6,
2026-03-24, still ships only the Unsafe-based package); at that point this
directory should be deleted and imports switched to the released artifact.
Tracking: no upstream issue or PR is on file yet - open one and link it here.

## What BeakGraph uses

Only `ZstdCompressor.create()` / `ZstdDecompressor.create()` and the `byte[]`
compress / decompress overloads (through `com.ebremer.beakgraph.utils.StringUtils`,
one instance per writer / per reader thread), plus `getDecompressedSize` for
validating a fragment's declared length.

## Intentionally not vendored

* `ZstdCodec.java` - extends `io.airlift.compress.v3.hadoop.CodecAdapter`, which
  implements `org.apache.hadoop.io.compress.CompressionCodec`; upstream compiles it
  against a `provided` Hadoop dependency that BeakGraph neither has nor wants.
* `ZstdHadoopStreams.java`, `ZstdHadoopInputStream.java`, `ZstdHadoopOutputStream.java`,
  `ZstdInputStream.java`, `ZstdOutputStream.java`, `ZstdIncrementalFrameDecompressor.java`
  - the streaming / incremental / Hadoop layer. Nothing in BeakGraph calls it, it
  was untested here, and it kept a compile-time coupling to `hadoop.*` abstract
  classes. Re-copy them if streaming is ever wanted (with a round-trip test).

## Local divergences from the fork

Kept minimal and listed so a diff against the fork is explainable; each should be
upstreamed to the fork:

* `Huffman.reset()` and its call from `ZstdFrameDecompressor.reset()`: the Huffman
  table is cleared at the start of every frame, so a treeless-literals block that
  opens a frame is rejected instead of decoding against the previous frame's table
  (RFC 8878 3.1.1.3.1.2; the decoder instance is reused across fragments).
* `Huffman.readTable`: weights and the decoded symbol count are checked with
  `verify()` before they index `ranks[]` / `weights[]` (corrupt input used to raise
  `ArrayIndexOutOfBoundsException` instead of `MalformedInputException`).
* `FseTableReader.readFseTable` / `FseCompressionTable.spreadSymbols`: `int` loop
  variables (a `byte` loop wrapped negative for more than 128 symbols).
* `ZstdFrameDecompressor`: three copy-pasted `verify()` messages corrected
  (block-size limit, offset-codes table, literals-length table).
* `XxHash64` header comment and the FFM 3-byte block-header fallback (already in
  the fork branch, not in upstream).
* Compression-context reuse (BG-171): `ZstdJavaCompressor` keeps one
  `CompressionContext` per window log and `ZstdFrameCompressor.compress` takes
  that cache (a null cache keeps the old allocate-per-frame path); a reused
  context is `reset(baseAddress)` - repeat offsets back to (1, 4), hash/chain
  tables cleared and the window restarted (`BlockCompressionState.reset(long)`),
  both Huffman tables invalidated (`HuffmanCompressionTable.invalidate()`, so a
  frame never opens with a treeless-literals block) and the sequence store
  emptied - and `FseCompressionTable.initialize` uses instance scratch arrays
  instead of allocating two per call (the two TODOs). Output is byte-identical to
  a fresh context (`ZstdCodecInteropTest.aReusedCompressorMatchesFreshFramesByteForByte`).
  Consequence: `ZstdJavaCompressor` instances are not thread-safe; BeakGraph
  holds one `StringUtils` per writer and per reader thread.

## Re-vendoring

    git -C ../aircompressor checkout <fork commit>
    rsync -a --delete --exclude ZstdCodec.java --exclude 'ZstdHadoop*' \
          --exclude ZstdInputStream.java --exclude ZstdOutputStream.java \
          --exclude ZstdIncrementalFrameDecompressor.java --exclude README.md \
          ../aircompressor/src/main/java/io/airlift/compress/v3/zstdFFM/ \
          src/main/java/io/airlift/compress/v3/zstdFFM/
    git diff --stat    # expect only the divergences above to reappear as removals

The classes still depend on the `aircompressor-v3` jar for the shared
`Compressor`/`Decompressor` interfaces, `MalformedInputException`, and
`internal.NativeLoader`.

License: Apache License 2.0 (headers retained in each file).
