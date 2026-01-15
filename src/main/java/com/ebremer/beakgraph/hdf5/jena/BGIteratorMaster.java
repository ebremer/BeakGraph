package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

public class BGIteratorMaster implements Iterator<BindingNodeId> {
    private final Iterator<BindingNodeId> chain;

    public BGIteratorMaster(HDF5Reader reader, PositionalDictionaryReader dict, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
    //    IO.println("BGIteratorMaster Check: " + quad);
        ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
        
        // --- Bound Checks ---
        boolean gBound = !quad.getGraph().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getGraph())));
        boolean sBound = !quad.getSubject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getSubject())));
        boolean pBound = !quad.getPredicate().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getPredicate())));
        boolean oBound = !quad.getObject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getObject())));        
        
     //   IO.println("  Bindings: G=" + gBound + " S=" + sBound + " P=" + pBound + " O=" + oBound);

        if (gBound) {
            if (pBound) {
                if (sBound) {
                    // G, P, S bound -> Find O (Index: GSPO)
                //    IO.println("  Strategy: GSPO (Find O)");
                    IndexReader gspo = reader.getIndexReader(Index.GSPO);
                    if (gspo != null) {
                         its.add(new BGIteratorSO(dict, gspo, bnid, quad, filter, nodeTable));
                    } else {
                        IO.println("  ERROR: GSPO IndexReader is NULL");
                    }
                } else {
                    if (oBound) {
                        // G, P, O bound -> Find S (Index: GPOS)
                 //       IO.println("  Strategy: GPOS (Find S)");
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorOS(dict, gpos, bnid, quad, filter, nodeTable));
                        } else {
                            IO.println("  ERROR: GPOS IndexReader is NULL");
                        }
                    } else {
                        // G, P bound -> Find S, O (Index: GPOS)
                 //       IO.println("  Strategy: GPOS (Find S, O)");
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorPOS(dict, gpos, bnid, quad, filter, nodeTable));
                        } else {
                            IO.println("  ERROR: GPOS IndexReader is NULL");
                        }
                    }
                }
            } else {
                // G bound, P variable -> Scan SP (Index: GSPO)
          //      IO.println("  Strategy: GSPO (Scan P)");
                IndexReader gspo = reader.getIndexReader(Index.GSPO);
                if (gspo != null) {
                    its.add(new BGIteratorSPO_All(dict, gspo, bnid, quad, filter, nodeTable));
                } else {
                    IO.println("  ERROR: GSPO IndexReader is NULL");
                }
            }
        } else {
            // G Variable -> Scan Graphs
       //     IO.println("  Strategy: Scan All Graphs");
            dict.getGraphs().streamNodes().forEach(n ->
                its.add(new BGIteratorMaster(reader, dict, bnid, new Quad(n, quad.getSubject(), quad.getPredicate(), quad.getObject()), filter, nodeTable))
            );                    
        }
        
        chain = new IteratorChain<>(its);
      //  if (!chain.hasNext()) {
        //    IO.println("  Master Chain is EMPTY (No results expected)");
        //}
    }

    @Override
    public boolean hasNext() {
        return chain.hasNext();
    }

    @Override
    public BindingNodeId next() {
        return chain.next();
    }
}