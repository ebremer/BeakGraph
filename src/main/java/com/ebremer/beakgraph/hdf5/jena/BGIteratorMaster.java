package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

public class BGIteratorMaster implements Iterator<BindingNodeId> {
    private final Iterator<BindingNodeId> chain;

    public BGIteratorMaster(HDF5Reader reader, PositionalDictionaryReader dict, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
        boolean gBound = !quad.getGraph().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getGraph())));
        boolean sBound = !quad.getSubject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getSubject())));
        boolean pBound = !quad.getPredicate().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getPredicate())));
        boolean oBound = !quad.getObject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getObject())));        

        if (gBound) {
            if (pBound) {
                if (sBound) {
                    // G, P, S bound -> Find O (Index: GSPO)
                    IndexReader gspo = reader.getIndexReader(Index.GSPO);
                    if (gspo != null) {
                         its.add(new BGIteratorSO(dict, gspo, bnid, quad, filter, nodeTable));
                    } else {
                        throw new IllegalStateException("Required GSPO index is missing from this BeakGraph file");
                    }
                } else {
                    if (oBound) {
                        // G, P, O bound -> Find S (Index: GPOS)
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorOS(dict, gpos, bnid, quad, filter, nodeTable));
                        } else {
                            throw new IllegalStateException("Required GPOS index is missing from this BeakGraph file");
                        }
                    } else {
                        // G, P bound -> Find S, O (Index: GPOS)
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            its.add(new BGIteratorPOS(dict, gpos, bnid, quad, filter, nodeTable));
                        } else {
                            throw new IllegalStateException("Required GPOS index is missing from this BeakGraph file");
                        }
                    }
                }
            } else {
                // G bound, P variable -> Scan SP (Index: GSPO)
                IndexReader gspo = reader.getIndexReader(Index.GSPO);
                if (gspo != null) {
                    its.add(new BGIteratorSPO_All(dict, gspo, bnid, quad, filter, nodeTable));
                } else {
                    throw new IllegalStateException("Required GSPO index is missing from this BeakGraph file");
                }
            }
        } else {
            // G is an unbound variable. Scan only the actual graphs (the columnar
            // `graphs` list), not every entity: getGraphs().streamNodes() would also
            // yield every URI/BNode in S/O positions, creating one (almost always empty)
            // sub-iterator per entity. Bind the graph variable to each graph too, since
            // each per-graph sub-iterator only ever sees a concrete graph.
            Var gVar = Var.alloc(quad.getGraph());
            dict.streamGraphs().forEach(n -> {
                Iterator<BindingNodeId> sub = new BGIteratorMaster(reader, dict, bnid,
                        new Quad(n, quad.getSubject(), quad.getPredicate(), quad.getObject()), filter, nodeTable);
                NodeId gId = new NodeId(dict.getGraphs().locate(n), NodeType.GRAPH);
                its.add(Iter.map(sub, b -> { b.put(gVar, gId); return b; }));
            });
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
