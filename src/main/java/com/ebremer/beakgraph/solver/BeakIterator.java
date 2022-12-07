package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.DataType;
import static com.ebremer.beakgraph.rdf.DataType.RESOURCE;
import com.ebremer.beakgraph.rdf.NodeTable;
import com.ebremer.beakgraph.rdf.VectorSearch;
import com.ebremer.beakgraph.store.NodeId;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.arrow.algorithm.search.VectorRangeSearcher;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;

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
    private final NodeTable nodeTable;
    
    public BeakIterator(BindingNodeId bnid, DataType datatype, StructVector dual, Triple triple, ExprList filter, NodeTable nodeTable) {
       // System.out.println("BeakIterator =========================\n"
         //       +bnid+"\n===[ "+triple.getSubject().isVariable()
           //     +" ]==== Triple : \n"+triple+"\n F ---> "+filter+"\n=== END ======");
       // if (triple.getSubject().isVariable()) System.out.println("Subject : "+triple.getSubject().getName());
       // if (triple.getObject().isVariable()) System.out.println("Object : "+triple.getObject().getName());
        this.nodeTable = nodeTable;
        boolean v = false;
        if ("http://www.w3.org/ns/oa#hasSelector".equals(triple.getPredicate().getURI())) {
            v = true;
        }
        if (bnid.containsKey(Var.alloc(triple.getSubject()))) {
         //   System.out.println("Setting index to SO");
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
            if (!triple.getObject().isVariable()) {
                int tar = nodeTable.getID(triple.getObject().getURI());
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
                if (v) {
                    IntStream.range(0, s.getValueCount()).forEach(g->{
                        System.out.println(tar+" "+g+" = "+s.get(g));
                    });
                    int zz = 0;
                }
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
                              //  System.out.println("PA : "+datatype+" "+triple);
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
        //System.out.println("hasNext() "+ i+" "+low+"  "+high+" "+triple);
        return (scan&&(i<=high));
    }

    @Override
    public BindingNodeId next() {
       // System.out.println(bnid+" NEXT ["+i+","+low+"->"+high+"] =-=-=-=> "+triple+" NEXT "+i+" "+hasNext());
        /*
        Binding pb = bnid.getParentBinding();
        pb.forEach((v,n)->{
            System.out.println("analysis : "+v+" "+n);
        
        });*/
        BindingNodeId neo = new BindingNodeId(bnid);
        if (triple.getObject().isVariable()) {
            Var c = Var.alloc(triple.getObject().getName());
            if (datatype == RESOURCE) {
                int rr = (int) pa.getChild("o").getObject(i);
                neo.put(c, new NodeId(rr));
            } else {
                neo.put(c, new NodeId(pa.getChild("o").getObject(i)));
            }
        } else { }
        if (triple.getSubject().isVariable()) {
            Var ss = Var.alloc(triple.getSubject().getName());
            if (!neo.containsKey(ss)) {
                int rr2 = (int) pa.getChild("s").getObject(i);
                neo.put(ss, new NodeId(rr2));
            }
        } else {
            System.out.println("NOT A VARIABLE");
        }
        i++;
        return neo;
    }
}
