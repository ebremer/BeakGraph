package com.ebremer.beakgraph.huge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.graph.NodeFactory;

/**
 * Compact, exact round-trip serialization of RDF terms for the spill files.
 * Term identity is preserved: URIs and blank node labels verbatim; literals as
 * lexical form + datatype URI (or + language tag), reconstructed through the
 * same {@code TypeMapper} path the HDF5 readers use, so a deserialized node is
 * {@code equals()} to - and {@code NodeComparator}-orders identically to - the
 * parsed original.
 *
 * <p>Node kinds the BeakGraph format cannot store (RDF-star triple terms,
 * variables) fail loudly here, matching the RAM writer's ProcessQuad guards.
 *
 * @author Erich Bremer
 */
public final class NodeCodec implements ExternalSorter.Codec<Node> {

    static final NodeCodec INSTANCE = new NodeCodec();

    private static final TypeMapper TM = TypeMapper.getInstance();
    private static final byte T_URI = 0;
    private static final byte T_BNODE = 1;
    private static final byte T_LITERAL_DT = 2;
    private static final byte T_LITERAL_LANG = 3;
    private static final byte T_LITERAL_DIRLANG = 4;

    private NodeCodec() {}

    @Override
    public void write(DataOutput out, Node n) throws IOException {
        writeNode(out, n);
    }

    @Override
    public Node read(DataInput in) throws IOException {
        return readNode(in);
    }

    public static void writeNode(DataOutput out, Node n) throws IOException {
        if (n.isURI()) {
            out.writeByte(T_URI);
            writeString(out, n.getURI());
        } else if (n.isBlank()) {
            out.writeByte(T_BNODE);
            writeString(out, n.getBlankNodeLabel());
        } else if (n.isLiteral()) {
            TextDirection dir = n.getLiteralBaseDirection();
            String lang = n.getLiteralLanguage();
            if (dir != null) {
                out.writeByte(T_LITERAL_DIRLANG);
                writeString(out, n.getLiteralLexicalForm());
                writeString(out, lang);
                out.writeByte(dir == TextDirection.LTR ? 1 : 2);
            } else if (lang != null && !lang.isEmpty()) {
                out.writeByte(T_LITERAL_LANG);
                writeString(out, n.getLiteralLexicalForm());
                writeString(out, lang);
            } else {
                out.writeByte(T_LITERAL_DT);
                writeString(out, n.getLiteralLexicalForm());
                writeString(out, n.getLiteralDatatypeURI());
            }
        } else {
            // Same stance as ProcessQuad: a node kind the store cannot represent
            // must abort the build, not silently skew it.
            throw new IllegalStateException("Unsupported node kind in huge writer spill: " + n);
        }
    }

    public static Node readNode(DataInput in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case T_URI -> NodeFactory.createURI(readString(in));
            case T_BNODE -> NodeFactory.createBlankNode(readString(in));
            case T_LITERAL_DT -> {
                String lex = readString(in);
                String dt = readString(in);
                yield NodeFactory.createLiteralDT(lex, TM.getSafeTypeByName(dt));
            }
            case T_LITERAL_LANG -> {
                String lex = readString(in);
                String lang = readString(in);
                yield NodeFactory.createLiteralLang(lex, lang);
            }
            case T_LITERAL_DIRLANG -> {
                String lex = readString(in);
                String lang = readString(in);
                byte dir = in.readByte();
                if (dir != 1 && dir != 2) {
                    throw new IOException("Corrupt spill record: base direction " + dir);
                }
                yield NodeFactory.createLiteralDirLang(lex, lang,
                        dir == 1 ? TextDirection.LTR : TextDirection.RTL);
            }
            default -> throw new IOException("Corrupt spill record: unknown node tag " + tag);
        };
    }

    static void writeString(DataOutput out, String s) throws IOException {
        // Explicit varint + UTF-8 bytes: DataOutput.writeUTF caps at 65535 bytes,
        // far too small for WKT literals.
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        HugeIO.writeVarLong(out, bytes.length);
        out.write(bytes);
    }

    static String readString(DataInput in) throws IOException {
        long len = HugeIO.readVarLong(in);
        if (len < 0 || len > Integer.MAX_VALUE - 8) {
            throw new IOException("Corrupt spill record: string length " + len);
        }
        return new String(HugeIO.readBytes(in, (int) len), StandardCharsets.UTF_8);
    }
}

