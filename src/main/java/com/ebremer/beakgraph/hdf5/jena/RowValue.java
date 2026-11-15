package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprTransformCopy;
import org.apache.jena.sparql.expr.ExprTransformer;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDecimal;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.apache.jena.sparql.expr.nodevalue.NodeValueFloat;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.sparql.function.FunctionEnv;

/**
 * A variable in an expression that one thread evaluates over the rows it
 * reads: its value for a row is found by the row's id, in a small table of
 * the values this thread last made.
 *
 * <p>Jena turns the term a variable is bound to into a value through one
 * cache for the whole JVM ({@code NodeValue.makeNode}: 10,000 entries). A
 * filter or an aggregate over many different terms - the subjects of a graph,
 * its names, its measurements - writes that cache and evicts from it for
 * every row, and several threads doing so wait for each other: of a count of
 * 2,000,000 rows through {@code FILTER(isIRI(?s))} on every core, 65% of the
 * processor time was that cache (PERFORMANCE.md). Here:
 * <ul>
 *   <li><b>A value asked again</b> - the same number in many rows, the same
 *       type - is the object made the last time, found by its id: no term is
 *       read and no value made.</li>
 *   <li><b>A value asked first</b> is made as Jena makes it, without the
 *       cache, where that is one step that Jena's public classes offer
 *       ({@link #valueOf}): an IRI or a blank node, a string, an integer, a
 *       decimal, a double, a float. Anything else - a date, a boolean, a
 *       language-tagged string, a literal that is not well formed - is
 *       Jena's to make, through its cache as before.</li>
 * </ul>
 * The values are the ones Jena would have made - of the same classes, with
 * the same number and the same term - so an expression cannot tell; a test
 * holds {@link #valueOf} to {@code NodeValue.makeNode} term by term.
 *
 * <p>For a binding that is not a row of ids of this store, or that has a
 * parent holding variables, it is Jena's variable.
 */
final class RowValue extends ExprVar {

    private static final int BITS = 10;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;

    /** The datatypes Jena reads as an integer of any size. */
    private static final Set<RDFDatatype> INTEGERS = Set.of(
            XSDDatatype.XSDinteger, XSDDatatype.XSDnonPositiveInteger, XSDDatatype.XSDnonNegativeInteger,
            XSDDatatype.XSDpositiveInteger, XSDDatatype.XSDnegativeInteger, XSDDatatype.XSDbyte, XSDDatatype.XSDshort,
            XSDDatatype.XSDint, XSDDatatype.XSDlong, XSDDatatype.XSDunsignedByte, XSDDatatype.XSDunsignedShort,
            XSDDatatype.XSDunsignedInt, XSDDatatype.XSDunsignedLong);

    private final NodeTable nodeTable;
    private final long[] ids = new long[1 << BITS];              // no id is 0
    private final NodeValue[] values = new NodeValue[1 << BITS];

    private RowValue(Var v, NodeTable nodeTable) {
        super(v);
        this.nodeTable = nodeTable;
    }

    /**
     * Not, as far as Jena's shortcuts go. Where a variable is the whole of
     * an expression - {@code MAX(?n)}, {@code BOUND(?x)} - Jena does not
     * evaluate it but reads the binding and makes the value itself, through
     * its cache ({@code ExprLib.evalOrNull}); asked this, it evaluates,
     * which is {@link #eval} here and gives the same value, or the same
     * exception for a variable that is not bound.
     */
    @Override
    public boolean isVariable() {
        return false;
    }

    @Override
    public NodeValue eval(Binding binding, FunctionEnv env) {
        if (binding instanceof BindingBG row && row.getGraph().getReader().getNodeTable() == nodeTable) {
            BindingNodeId idsOfRow = row.getBindingId();
            Binding parent = idsOfRow.getParentBinding();
            if (parent == null || parent.isEmpty()) {
                long id = idsOfRow.get(varNode);
                if (id != NodeId.NONE && !NodeId.isSpecial(id)) {
                    int slot = (int) ((id * GOLDEN) >>> (64 - BITS));
                    if (ids[slot] == id) {
                        return values[slot];
                    }
                    Node term = nodeTable.getNodeForNodeId(id);
                    if (term != null) {
                        NodeValue value = valueOf(term);
                        values[slot] = value;
                        ids[slot] = id;
                        return value;
                    }
                }
            }
        }
        return super.eval(binding, env);
    }

    /**
     * The value of {@code term}, as {@code NodeValue.makeNode} gives it. The
     * common kinds are made here the way Jena's own makers make them
     * ({@code NVFactory}: a well-formed literal's number or string beside its
     * term), which keeps them out of Jena's cache; every other kind, and any
     * literal that is not well formed, is handed to Jena.
     */
    static NodeValue valueOf(Node term) {
        if (!term.isLiteral()) {
            return new NodeValueNode(term);
        }
        RDFDatatype datatype = term.getLiteralDatatype();
        if (XSDDatatype.XSDstring.equals(datatype)) {
            return new NodeValueString(term.getLiteralLexicalForm(), term);
        }
        LiteralLabel literal = term.getLiteral();
        if (literal.isWellFormed()) {
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
     * {@code expr} in objects no other thread holds - an expression may keep
     * what it has worked out, and Jena evaluates one on one thread at a time -
     * with its variables read as {@link RowValue}s of {@code nodeTable}'s
     * store.
     */
    static Expr own(Expr expr, NodeTable nodeTable) {
        return ExprTransformer.transform(new ExprTransformCopy() {
            @Override
            public Expr transform(ExprVar v) {
                return new RowValue(v.asVar(), nodeTable);
            }
        }, expr.deepCopy());
    }

    /** {@link #own(Expr, NodeTable)}, of each expression of a list. */
    static ExprList own(ExprList exprs, NodeTable nodeTable) {
        ExprList copies = new ExprList();
        for (Expr e : exprs) {
            copies.add(own(e, nodeTable));
        }
        return copies;
    }
}
