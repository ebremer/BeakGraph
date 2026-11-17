package com.ebremer.beakgraph.core.lib;

import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.sparql.SystemARQ;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDecimal;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.apache.jena.sparql.expr.nodevalue.NodeValueFloat;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

/**
 * The value Jena gives a term, made without Jena's cache where that can be
 * done from Jena's public classes.
 *
 * <p>{@code NodeValue.makeNode} finds a term's value in ONE cache of 10,000
 * entries for the whole JVM. Work that asks for the values of many different
 * terms - a sort of a store's literals, a filter over its rows - writes that
 * cache and evicts from it for every term, and threads doing so wait for each
 * other (PERFORMANCE.md). {@link #of} makes the common kinds as Jena's own
 * makers do ({@code NVFactory}, which is not public): the value is of the
 * same class, with the same number or string and the same term, so nothing
 * that reads it can tell. {@code NodeValuesTest} holds it to
 * {@code NodeValue.makeNode} term by term.
 */
public final class NodeValues {

    private NodeValues() {}

    static {
        // NodeValue makes values of its own subclasses while it is initialized
        // (its constants), and each subclass waits for NodeValue before it is.
        // A thread that is first to touch a subclass - which only code that
        // makes values itself, as here, can be - and a thread initializing
        // NodeValue then wait for each other for good: a build's sort threads
        // did, the first time a JVM's first value was made here. Jena's own
        // entry initializes NodeValue first; so does this class, before any
        // thread can make a value through it.
        java.util.Objects.requireNonNull(NodeValue.TRUE);
    }

    /** The datatypes Jena reads as an integer of any size. */
    private static final Set<RDFDatatype> INTEGERS = Set.of(
            XSDDatatype.XSDinteger, XSDDatatype.XSDnonPositiveInteger, XSDDatatype.XSDnonNegativeInteger,
            XSDDatatype.XSDpositiveInteger, XSDDatatype.XSDnegativeInteger, XSDDatatype.XSDbyte, XSDDatatype.XSDshort,
            XSDDatatype.XSDint, XSDDatatype.XSDlong, XSDDatatype.XSDunsignedByte, XSDDatatype.XSDunsignedShort,
            XSDDatatype.XSDunsignedInt, XSDDatatype.XSDunsignedLong);

    /**
     * The value of {@code term}, as {@code NodeValue.makeNode} gives it. Made
     * here: a term that is not a literal, a string, a well-formed integer,
     * decimal, double or float, and a literal of a datatype Jena has no value
     * for ({@link #isOpaque}). Every other kind - a date, a boolean, a
     * language-tagged string, a literal that is not well formed - is handed
     * to Jena, cache and all.
     */
    public static NodeValue of(Node term) {
        if (!term.isLiteral()) {
            return new NodeValueNode(term);
        }
        RDFDatatype datatype = term.getLiteralDatatype();
        if (XSDDatatype.XSDstring.equals(datatype)) {
            return new NodeValueString(term.getLiteralLexicalForm(), term);
        }
        if (isOpaque(datatype)) {
            return new NodeValueNode(term);
        }
        LiteralLabel literal = term.getLiteral();
        // Jena's own class for the XSD types: what is repeated below is how
        // it makes a value of one of THOSE. A datatype of another class that
        // only carries an XSD IRI is Jena's to deal with.
        if (datatype instanceof XSDDatatype && literal.isWellFormed()) {
            if (XSDDatatype.XSDdouble.equals(datatype)) {
                return new NodeValueDouble(((Number) literal.getValue()).doubleValue(), term);
            }
            if (XSDDatatype.XSDfloat.equals(datatype)) {
                return new NodeValueFloat(((Number) literal.getValue()).floatValue(), term);
            }
            if (XSDDatatype.XSDdecimal.equals(datatype)) {
                return new NodeValueDecimal(new BigDecimal(term.getLiteralLexicalForm().trim()), term);
            }
            if (INTEGERS.contains(datatype)) {
                String lexical = term.getLiteralLexicalForm().trim();
                if (datatype.isValid(lexical)) {
                    return new NodeValueInteger(new BigInteger(lexical), term);
                }
            }
        }
        return NodeValue.makeNode(term);
    }

    /**
     * Whether Jena has no value for a literal of {@code datatype}, so that the
     * literal is its own value: a {@code NodeValueNode}, whatever its lexical
     * form. Jena's makers are keyed by datatype, and a datatype equals another
     * of the same IRI; the ones it has makers for are XSD's, RDF's two
     * language-string types and, when switched on, its Roman numerals. So a
     * datatype is opaque when it is one Jena made for an IRI it did not know
     * (a plain {@code BaseDatatype}) or is the WKT literal's, and its IRI is
     * in neither of those namespaces. Any other class of datatype - RDF's
     * XMLLiteral, HTML and JSON, the composite types - is left to Jena.
     */
    static boolean isOpaque(RDFDatatype datatype) {
        if (datatype == null || SystemARQ.EnableRomanNumerals) {
            return false;
        }
        if (datatype.getClass() != BaseDatatype.class && !(datatype instanceof WKTDatatype)) {
            return false;
        }
        String iri = datatype.getURI();
        return iri != null && !iri.startsWith(XSD.NS) && !iri.startsWith(RDF.uri);
    }
}
