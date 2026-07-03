# Vendored: io.airlift.compress.v3.zstdFFM

Copied verbatim (package name preserved) from
https://github.com/ebremer/aircompressor/tree/zstdFFM/src/main/java/io/airlift/compress/v3/zstdFFM
on 2026-07-02.

This is the FFM (Foreign Function & Memory API) port of aircompressor's pure-Java
Zstd codec — it replaces the sun.misc.Unsafe-based implementation in
`io.airlift.compress.v3.zstd`. It is vendored here TEMPORARILY until the upstream
aircompressor project publishes its own release containing this package, at which
point this directory should be deleted and imports switched to the released
artifact.

One file is intentionally NOT vendored: `ZstdCodec.java`. It extends
`io.airlift.compress.v3.hadoop.CodecAdapter`, which implements
`org.apache.hadoop.io.compress.CompressionCodec` — upstream compiles it against
a `provided` Hadoop dependency that BeakGraph neither has nor wants. BeakGraph
does not use Hadoop compression codecs.

Otherwise, do not modify these files locally; make changes in the aircompressor fork and
re-copy. The classes still depend on the `aircompressor-v3` jar for the shared
`Compressor`/`Decompressor` interfaces, `MalformedInputException`, and
`internal.NativeLoader`.

License: Apache License 2.0 (headers retained in each file).
