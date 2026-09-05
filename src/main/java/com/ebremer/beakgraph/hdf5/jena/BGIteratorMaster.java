package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import java.util.Collections;
import java.util.Iterator;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

public class BGIteratorMaster implements Iterator<BindingNodeId> {
    // Every dispatch branch selects exactly ONE concrete iterator; this class is
    // pure routing (the former per-call ArrayList + IteratorChain wrapper was
    // constructed once per input binding for nothing).
    private final Iterator<BindingNodeId> chain;

    public BGIteratorMaster(HDF5Reader reader, PositionalDictionaryReader dict, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        // THE term classifier (the one-classifier rule's (CHANGELOG.md "Format v5 design notes") fix - one classification, here):
        // - A CONCRETE triple term is a bound term like any other; locate()
        //   answers it (a miss is correctly empty), so no special routing.
        // - A VAR-CONTAINING triple term in the object position is
        //   unbound-with-unification: routed below as if the object were a
        //   variable; the chosen iterator compiles a TripleTermMatcher.
        // - A var-containing triple term anywhere else can never match data
        //   (RDF 1.2 permits triple terms in object position only), and probing
        //   a dictionary comparator with embedded variables is undefined -
        //   answer empty here rather than let an iterator improvise.
        if (TripleTermMatcher.isPattern(quad.getGraph())
                || TripleTermMatcher.isPattern(quad.getSubject())
                || TripleTermMatcher.isPattern(quad.getPredicate())) {
            chain = Collections.emptyIterator();
            return;
        }
        boolean gBound = !quad.getGraph().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getGraph())));
        boolean sBound = !quad.getSubject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getSubject())));
        boolean pBound = !quad.getPredicate().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getPredicate())));
        boolean oBound = !TripleTermMatcher.isPattern(quad.getObject())
                && (!quad.getObject().isVariable() || (bnid!=null && bnid.containsKey(Var.alloc(quad.getObject()))));

        if (gBound) {
            if (pBound) {
                if (sBound) {
                    // G, P, S bound -> Find O (Index: GSPO)
                    IndexReader gspo = reader.getIndexReader(Index.GSPO);
                    if (gspo != null) {
                         chain = new BGIteratorSO(dict, gspo, bnid, quad, filter, nodeTable);
                    } else {
                        throw new IllegalStateException("Required GSPO index is missing from this BeakGraph file");
                    }
                } else {
                    if (oBound) {
                        // G, P, O bound -> Find S (Index: GPOS)
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            chain = new BGIteratorOS(dict, gpos, bnid, quad, filter, nodeTable);
                        } else {
                            throw new IllegalStateException("Required GPOS index is missing from this BeakGraph file");
                        }
                    } else {
                        // G, P bound -> Find S, O (Index: GPOS)
                        IndexReader gpos = reader.getIndexReader(Index.GPOS);
                        if (gpos != null) {
                            chain = new BGIteratorPOS(dict, gpos, bnid, quad, filter, nodeTable);
                        } else {
                            throw new IllegalStateException("Required GPOS index is missing from this BeakGraph file");
                        }
                    }
                }
            } else {
                // G bound, P variable -> Scan SP (Index: GSPO)
                IndexReader gspo = reader.getIndexReader(Index.GSPO);
                if (gspo != null) {
                    chain = new BGIteratorSPO_All(dict, gspo, bnid, quad, filter, nodeTable);
                } else {
                    throw new IllegalStateException("Required GSPO index is missing from this BeakGraph file");
                }
            }
        } else {
            // G is an unbound variable. Scan only the actual graphs (the columnar
            // `graphs` list), not every entity - and LAZILY: constructing every
            // graph's sub-iterator up front paid each one's index binary searches
            // before the first row came back (spatial stores hold thousands of
            // tile graphs). The graph id is taken straight from the columnar list
            // (no extract() -> locate() round trip) and pre-bound in a child
            // binding: every concrete iterator resolves a pre-bound graph var,
            // the binding rides into every result row, and a pattern that also
            // uses the var (GRAPH ?g { ?g ?p ?o }) is constrained through the
            // ordinary bound-variable substitution instead of a post-filter.
            Var gVar = Var.alloc(quad.getGraph());
            // GRAPH/SUBJECT/OBJECT share the universal entity id-space, so a
            // pre-bound graph id is valid in those positions - but PREDICATE ids
            // live in an isolated dictionary. If the graph var also occupies the
            // predicate position, fall back to concrete-graph substitution with
            // the cross-space compatibility filter.
            boolean gVarInPredicate = quad.getPredicate().isVariable()
                    && quad.getPredicate().getName().equals(gVar.getName());
            if (gVarInPredicate) {
                Iterator<org.apache.jena.graph.Node> graphNodes = dict.streamGraphs().iterator();
                chain = Iter.flatMap(graphNodes, n -> {
                    long gId = NodeId.pack(NodeType.GRAPH, dict.getGraphs().locate(n));
                    Iterator<BindingNodeId> sub = new BGIteratorMaster(reader, dict, bnid,
                            new Quad(n, quad.getSubject(), quad.getPredicate(), quad.getObject()), filter, nodeTable);
                    return Iter.removeNulls(Iter.map(sub,
                            b -> b.putCompatible(gVar, gId, nodeTable) ? b : null));
                });
            } else {
                Iterator<Long> graphIds = dict.streamGraphIds()
                        .mapToObj(gid -> NodeId.pack(NodeType.GRAPH, gid)).iterator();
                chain = Iter.flatMap(graphIds, gId -> {
                    BindingNodeId child = new BindingNodeId(bnid);
                    child.put(gVar, gId);
                    return new BGIteratorMaster(reader, dict, child, quad, filter, nodeTable);
                });
            }
        }
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
