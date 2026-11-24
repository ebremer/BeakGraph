package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.lib.NodeValues;
import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.ValueSpace;
import org.apache.jena.sparql.util.ExprUtils;
import org.junit.jupiter.api.Test;

/**
 * The value a worker, or a sort, makes of a term ({@link NodeValues#of}) must be the
 * value Jena makes of it ({@code NodeValue.makeNode}): the same class, the
 * same term, the same place in the order of values and the same number.
 * Jena's makers are not public, and a few of them are repeated in
 * {@link NodeValues} to keep the common kinds of term out of Jena's cache; this
 * is what says they are repeated right, for the Jena on the class path.
 */
class RowValueTest {

    private static Node typed(String lexical, RDFDatatype datatype) {
        return NodeFactory.createLiteralDT(lexical, datatype);
    }

    private static List<Node> terms() {
        List<Node> terms = new ArrayList<>();
        terms.add(NodeFactory.createURI("http://ex.org/a"));
        terms.add(NodeFactory.createBlankNode("b1"));
        terms.add(NodeFactory.createLiteralString(""));
        terms.add(NodeFactory.createLiteralString("plain"));
        terms.add(NodeFactory.createLiteralString("  spaced  "));
        terms.add(NodeFactory.createLiteralLang("tagged", "en"));
        terms.add(NodeFactory.createLiteralDirLang("tagged", "ar", "rtl"));
        terms.add(typed("norm", XSDDatatype.XSDnormalizedString));
        terms.add(typed("true", XSDDatatype.XSDboolean));
        terms.add(typed("2026-11-15T10:00:00Z", XSDDatatype.XSDdateTime));
        terms.add(typed("2026-11-15", XSDDatatype.XSDdate));
        terms.add(typed("P1D", XSDDatatype.XSDduration));
        terms.add(typed("POINT(1 2)", TypeMapper.getInstance().getSafeTypeByName("http://www.opengis.net/ont/geosparql#wktLiteral")));
        terms.add(typed("anything", TypeMapper.getInstance().getSafeTypeByName("http://ex.org/datatype")));
        terms.add(typed("POLYGON ((0 0, 1 0, 1 1, 0 0))", WKTDatatype.INSTANCE));
        terms.add(typed("not wkt", WKTDatatype.INSTANCE));
        terms.add(typed("<p>x</p>", org.apache.jena.vocabulary.RDF.dtXMLLiteral));
        terms.add(typed("[1, 2]", NodeFactory.getType("http://w3id.org/awslabs/neptune/SPARQL-CDTs/List")));
        // Datatypes of Jena's plain class that only CARRY an IRI Jena has a maker for: Jena finds the maker by the IRI.
        terms.add(typed("5", new org.apache.jena.datatypes.BaseDatatype(XSDDatatype.XSDint.getURI())));
        terms.add(typed("abc", new org.apache.jena.datatypes.BaseDatatype(XSDDatatype.XSDint.getURI())));
        terms.add(typed("1.5", new org.apache.jena.datatypes.BaseDatatype(XSDDatatype.XSDdouble.getURI())));
        terms.add(typed("text", new org.apache.jena.datatypes.BaseDatatype(XSDDatatype.XSDstring.getURI())));
        for (String lexical : new String[] {"0", "1", "-1", "+5", "05", "007", " 12 ", "123456789012345678901234567890", "1.0", "abc", "", "1e3", "-0"}) {
            for (RDFDatatype datatype : new RDFDatatype[] {XSDDatatype.XSDinteger, XSDDatatype.XSDint, XSDDatatype.XSDlong, XSDDatatype.XSDshort,
                    XSDDatatype.XSDbyte, XSDDatatype.XSDnonNegativeInteger, XSDDatatype.XSDpositiveInteger, XSDDatatype.XSDnegativeInteger,
                    XSDDatatype.XSDnonPositiveInteger, XSDDatatype.XSDunsignedInt, XSDDatatype.XSDunsignedLong, XSDDatatype.XSDunsignedShort,
                    XSDDatatype.XSDunsignedByte}) {
                terms.add(typed(lexical, datatype));
            }
        }
        for (String lexical : new String[] {"0", "1", "1.5", "1.50", "-0.0", "+2.5", " 3.25 ", ".5", "5.", "1e3", "abc", "", "123456789012345678901234567890.123456789"}) {
            terms.add(typed(lexical, XSDDatatype.XSDdecimal));
        }
        for (String lexical : new String[] {"0", "1", "1.5", "0.5e0", "1e3", "1E3", "-0.0", "-0.0e0", "NaN", "INF", "-INF", "+INF", " 2.5 ", "abc", "",
                "1.7976931348623157E308", "4.9E-324", "0.1", "3.4028235E38", "1e-50"}) {
            terms.add(typed(lexical, XSDDatatype.XSDdouble));
            terms.add(typed(lexical, XSDDatatype.XSDfloat));
        }
        return terms;
    }

    @Test
    void aValueMadeHereIsTheValueJenaMakes() {
        int made = 0;
        for (Node term : terms()) {
            NodeValue jena;
            try {
                jena = NodeValue.makeNode(term);
            } catch (RuntimeException e) {
                // "abc" with a plain BaseDatatype carrying xsd:int's IRI: Jena's
                // integer maker parses it unguarded, so makeNode throws unless
                // its JVM-wide cache already holds an equal node, which depends
                // on the tests that ran before. NodeValues.of hands such a term
                // to Jena; whatever Jena does, it must do the same.
                RuntimeException ours = assertThrows(RuntimeException.class, () -> NodeValues.of(term), term.toString());
                assertSame(e.getClass(), ours.getClass(), term.toString());
                continue;
            }
            NodeValue ours = NodeValues.of(term);
            String what = term + " (" + jena.getClass().getSimpleName() + ")";
            assertSame(jena.getClass(), ours.getClass(), what);
            assertEquals(term, ours.asNode(), what);
            if (ours != jena) {
                assertSame(term, ours.asNode(), "made here, it holds the term it was made of: " + what);
            }
            assertEquals(jena, ours, what);
            assertEquals(jena.getValueSpace(), ours.getValueSpace(), what);
            assertEquals(jena.isNumber(), ours.isNumber(), what);
            assertEquals(jena.isString(), ours.isString(), what);
            assertEquals(0, NodeValue.compareAlways(jena, ours), what);
            if (jena.isInteger()) {
                assertEquals(jena.getInteger(), ours.getInteger(), what);
            }
            if (jena.isDecimal()) {
                assertEquals(jena.getDecimal(), ours.getDecimal(), what);
            }
            if (jena.isDouble()) {
                assertEquals(Double.doubleToLongBits(jena.getDouble()), Double.doubleToLongBits(ours.getDouble()), what);
            }
            if (jena.isFloat()) {
                assertEquals(Float.floatToIntBits(jena.getFloat()), Float.floatToIntBits(ours.getFloat()), what);
            }
            if (jena.isString()) {
                assertEquals(jena.getString(), ours.getString(), what);
            }
            if (ours != jena && jena.getValueSpace() != ValueSpace.VSPACE_UNKNOWN) {
                made++;
            }
        }
        assertTrue(made >= 60, made + " values with a meaning were made here and not by Jena");
    }

    @Test
    void anExpressionOfItsOwnAnswersAsTheOriginalDoes() {
        // Copied for one thread, with its variables read by the rows' ids: over a binding that is no such row, it is
        // the expression it was copied from.
        Expr expr = ExprUtils.parse("(?v + 1 > 3) && isIRI(?s) && regex(str(?s), '^http')");
        Expr own = RowValue.own(expr, null);
        assertEquals(expr, own);
        assertTrue(own != expr);
        org.apache.jena.sparql.engine.binding.Binding row = org.apache.jena.sparql.engine.binding.Binding.builder()
                .add(org.apache.jena.sparql.core.Var.alloc("v"), typed("5", XSDDatatype.XSDinteger))
                .add(org.apache.jena.sparql.core.Var.alloc("s"), NodeFactory.createURI("http://ex.org/a")).build();
        assertEquals(expr.isSatisfied(row, null), own.isSatisfied(row, null));
        assertTrue(own.isSatisfied(row, null));
    }
}
