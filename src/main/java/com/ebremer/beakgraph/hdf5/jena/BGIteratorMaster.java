package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.FiveSectionDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprList;

public class BGIteratorMaster implements Iterator<BindingNodeId> {
    private final Iterator<BindingNodeId> chain;

    public BGIteratorMaster(HDF5Reader reader, FiveSectionDictionaryReader dict, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
        
        boolean gBound = !quad.getGraph().isVariable();
        boolean sBound = !quad.getSubject().isVariable();
        boolean pBound = !quad.getPredicate().isVariable();
        boolean oBound = !quad.getObject().isVariable();
        
        if (gBound) {
            if (pBound) {
                if (sBound) {
                    // G, P, S bound -> Find O (Index: GSPO)
                    IndexReader gspo = reader.getIndexReader(Index.GSPO);
                    if (gspo != null) {
                         // BGIteratorSO finds O given G, S, P
                         its.add(new BGIteratorSO(dict, gspo, bnid, quad, filter, nodeTable));
                    }
                } else {
                    if (oBound) {
                        // G, P, O bound -> Find S (Index: GPOS)
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorOS(dict, gpos, bnid, quad, filter, nodeTable));
                        }
                    } else {
                        // G, P bound -> Find S, O (Index: GPOS)
                        // OPTIMIZATION: Use GPOS directly instead of scanning Object dictionary
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorPOS(dict, gpos, bnid, quad, filter, nodeTable));
                        }
                    }
                }
            } else {
                /*
                IndexReader gpos = reader.getIndexReader(Index.GPOS);
                if (gpos != null) {
                    its.add(new BGIteratorPOS_All(dict, gpos, bnid, quad, filter, nodeTable));
                }
*/
                IndexReader gspo = reader.getIndexReader(Index.GSPO);
                if (gspo != null) {
                    its.add(new BGIteratorSPO_All(dict, gspo, bnid, quad, filter, nodeTable));
                }
            }
        } else {
            // G Variable -> Scan Graphs
            dict.getGraphs().streamNodes().forEach(n ->
                its.add(new BGIteratorMaster(reader, dict, bnid, new Quad(n, quad.getSubject(), quad.getPredicate(), quad.getObject()), filter, nodeTable))
            );            
        }
        chain = new IteratorChain<>(its);
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