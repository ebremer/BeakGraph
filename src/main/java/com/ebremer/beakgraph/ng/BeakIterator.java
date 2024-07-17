package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.DataType.RESOURCE;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.algorithm.search.VectorRangeSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BeakIterator implements Iterator<BindingNodeId> {
    private int i;
    private int low;
    private int high;
    private final Triple triple;
    private final BindingNodeId bnid;
    private final StructVector pa;
    private final DataType datatype;
    private final boolean scan;
    private final Node predicate;
    private final NodeTable nodeTable;
    private static final Logger logger = LoggerFactory.getLogger(BeakIterator.class);
    
    public BeakIterator(BindingNodeId bnid, DataType datatype, StructVector dual, Triple triple, ExprList filter, NodeTable nodeTable, Node predicate) {
        this.nodeTable = nodeTable;
        this.predicate = predicate;
        if (!triple.getSubject().isVariable()) {
            this.pa = (StructVector) dual.getChild("so");
            int skey = nodeTable.getID(triple.getSubject());
            IntVector s = (IntVector) pa.getChild("s");
            try (IntVector search = new IntVector("search", dual.getAllocator())) {
                search.allocateNew(1);
                search.set(0, skey);
                search.setValueCount(1);
                VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                low = VectorRangeSearcher.getFirstMatch(s, comparator, search, 0 );
                high = VectorRangeSearcher.getLastMatch(s, comparator, search, 0 );
                scan = !((low<0)||(high<0));
                search.close();
            }
        } else if (bnid.containsKey(Var.alloc(triple.getSubject()))) {
            this.pa = (StructVector) dual.getChild("so");
            int skey = bnid.get(Var.alloc(triple.getSubject())).getID();
            IntVector s = (IntVector) pa.getChild("s");
            try (IntVector search = new IntVector("search", dual.getAllocator())) {
                search.allocateNew(1);
                search.set(0, skey);
                search.setValueCount(1);
                VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                low = VectorRangeSearcher.getFirstMatch(s, comparator, search, 0 );
                high = VectorRangeSearcher.getLastMatch(s, comparator, search, 0 );
                scan = !((low<0)||(high<0));
                search.close();
            }
        } else {
            this.pa = (StructVector) dual.getChild("os");
            if (pa==null) {
                System.out.println("HOLD");
            }
            if (!triple.getObject().isVariable()) {
                int tar = nodeTable.getID(triple.getObject());
                IntVector s = (IntVector) pa.getChild("o");
                try (IntVector search = new IntVector("search", dual.getAllocator())) {
                    search.allocateNew(1);
                    search.set(0, tar);
                    search.setValueCount(1);
                    VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                    low = VectorRangeSearcher.getFirstMatch(s, comparator, search, 0);
                    high = VectorRangeSearcher.getLastMatch(s, comparator, search, 0 );
                    scan = !((low<0)||(high<0));
                    search.close();
                }
            } else if (bnid.containsKey(Var.alloc(triple.getObject().getName()))) {
                int tar = bnid.get(Var.alloc(triple.getObject().getName())).getID();
                IntVector s = (IntVector) pa.getChild("o");
                try (IntVector search = new IntVector("search", dual.getAllocator())) {
                    search.allocateNew(1);
                    search.set(0, tar);
                    search.setValueCount(1);
                    VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                    low = VectorRangeSearcher.getFirstMatch(s, comparator, search, 0);
                    high = VectorRangeSearcher.getLastMatch(s, comparator, search, 0 );
                    scan = !((low<0)||(high<0));
                    search.close();
                }
            } else if (filter!=null) {
                this.low = 0;
                this.high = dual.getValueCount()-1;
                filter.forEach(c->{
                    if (c.getFunction().getOpName().compareTo(">=")==0) {
                        List<Expr> args = c.getFunction().getArgs();
                        if (args.size()==2) {
                            if (args.get(0).isVariable()) {
                                long tar = args.get(1).getConstant().getInteger().longValueExact();
                                BigIntVector s = (BigIntVector) pa.getChild("o");
                                try (BigIntVector search = new BigIntVector("search", dual.getAllocator())) {
                                    search.allocateNew(1);
                                    search.set(0, tar);
                                    search.setValueCount(1);
                                    VectorValueComparator<BigIntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                                    low = VectorSearch.getFirstOrClosestMatch(s, comparator, search, 0 );
                                    search.close();
                                }
                            } else {
                                
                            }
                        }                  
                    } else if (c.getFunction().getOpName().compareTo("<=")==0) {
                        List<Expr> args = c.getFunction().getArgs();
                        if (args.size()==2) {
                            if (args.get(0).isVariable()) {
                                long tar = args.get(1).getConstant().getInteger().longValueExact();
                                BigIntVector s = (BigIntVector) pa.getChild("o");
                                try (BigIntVector search = new BigIntVector("search", dual.getAllocator())) {
                                    search.allocateNew(1);
                                    search.set(0, tar);
                                    search.setValueCount(1);
                                    VectorValueComparator<BigIntVector> comparator = DefaultVectorComparators.createDefaultComparator(search);
                                    high = VectorSearch.getFirstOrLargestRight(s, comparator, search, 0 );
                                    search.close();
                                }
                            } else {
                                
                            }
                        }
                    } else {
                        System.out.println("What is this??! : "+c);
                    }
                });
                scan = !((low<0)||(high<0));
            } else {
                this.low = 0;
                this.high = dual.getValueCount()-1;
                scan = true;
            }
        }
        this.triple = triple;
        this.bnid = bnid;
        this.datatype = datatype;
        i = low;
    }

    @Override
    public boolean hasNext() {
        return (scan&&(i<=high));
    }

    @Override
    public BindingNodeId next() {
        BindingNodeId neo = new BindingNodeId(bnid);
        if (triple.getObject().isVariable()) {
            Var c = Var.alloc(triple.getObject().getName());
            if (datatype == RESOURCE) {
                int rr = (int) pa.getChild("o").getObject(i);
                neo.put(c, new NodeId(rr));
            } else {
                FieldVector fv = pa.getChild("o");
                Object ss = fv.getObject(i);
                neo.put(c, new NodeId(ss));
            }
        }
        ArrowBuf ha;
        if (triple.getSubject().isVariable()) {
            Var ss = Var.alloc(triple.getSubject().getName());
            if (!neo.containsKey(ss)) {
                try {
                int rr2 = (int) pa.getChild("s").getObject(i);
                neo.put(ss, new NodeId(rr2));
                } catch (IndexOutOfBoundsException ex) {
                    System.out.println("hold 2");
                }
            }
        }
        if (triple.getPredicate().isVariable()) {
            Var pp = Var.alloc(triple.getPredicate().getName());
            if (!neo.containsKey(pp)) {
                int ee = nodeTable.getID(predicate);
                if (ee<nodeTable.getid2IRI().getValueCount()) {
                    Node xx = nodeTable.getURINode(ee);
                    neo.put(pp, new NodeId(ee));
                } else {
                    System.out.println("PREDICATE MISSING : "+predicate);
                }
            }
        }
        i++;
        return neo;
    }
}
