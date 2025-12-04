package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeType;
import com.ebremer.beakgraph.core.NodeTable;
import io.jhdf.api.Group;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.LongStream;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

/**
 * Iterator that traverses a Bit-Packed HDT Index and returns BindingNodeIds.
 * * @author Erich Bremer
 */
public class BGIterator implements Iterator<BindingNodeId> {
    private final BindingNodeId parentBinding;
    private final ExprList filter;
    private final NodeTable nodeTable; 
    private final Group hdt;
    private final PositionalDictionaryReader dict;
    private final Quad queryQuad;
    
    // Search Ranges
    private long Di; // Current cursor in the Leaf level
    private long Dj; // End of the Leaf level range
    
    private Index optimalIndex;
    private IndexReader selectedReader;
    private boolean hasNext = false;
    private final char[] positions;

    public BGIterator(PositionalDictionaryReader dict, IndexReader defaultReader, Group hdt, BindingNodeId bnid, Quad quad, ExprList filter, NodeTable nodeTable) {
        this.parentBinding = bnid;
        this.hdt = hdt;
        this.queryQuad = quad;
        this.filter = filter;
        this.nodeTable = nodeTable;
        this.dict = dict;
        
        // 1. Determine Bound Variables
        boolean gBound = !quad.getGraph().isVariable();
        boolean sBound = !quad.getSubject().isVariable();
        boolean pBound = !quad.getPredicate().isVariable();
        boolean oBound = !quad.getObject().isVariable();        
        
        // 2. Select Optimal Index
        optimalIndex = selectOptimalIndex(gBound, sBound, pBound, oBound);        
        selectedReader = getIndexReader(optimalIndex);
        
        if (selectedReader == null) {
            selectedReader = defaultReader; 
            optimalIndex = defaultReader.getIndexType();
        }        
        this.positions = optimalIndex.name().toCharArray();

        // 3. Resolve Dictionary IDs
        long gId = gBound ? dict.getGraphs().locate(quad.getGraph()) : -1;
        long sId = sBound ? dict.getSubjects().locate(quad.getSubject()) : -1;
        long pId = pBound ? dict.getPredicates().locate(quad.getPredicate()) : -1;
        long oId = oBound ? dict.getObjects().locate(quad.getObject()) : -1;

        // If a bound node is missing from dictionary, no results exist.
        if ((gBound && gId == -1) || (sBound && sId == -1) || 
            (pBound && pId == -1) || (oBound && oId == -1)) {
            this.hasNext = false;
        } else {            
            initSearch(selectedReader, gId, sId, pId, oId);
        }
    }
     
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext) throw new NoSuchElementException();
        QuadID qid = extractQuadIdAtIndex(selectedReader, positions, Di);
        BindingNodeId result = new BindingNodeId(this.parentBinding);
        if (queryQuad.getGraph().isVariable()) {
            result.put(Var.alloc(queryQuad.getGraph()), new NodeId(qid.g, NodeType.GRAPH));
        }
        if (queryQuad.getSubject().isVariable()) {
            result.put(Var.alloc(queryQuad.getSubject()), new NodeId(qid.s, NodeType.SUBJECT));
        }
        if (queryQuad.getPredicate().isVariable()) {
            result.put(Var.alloc(queryQuad.getPredicate()), new NodeId(qid.p, NodeType.PREDICATE));
        }
        if (queryQuad.getObject().isVariable()) {
            result.put(Var.alloc(queryQuad.getObject()), new NodeId(qid.o, NodeType.OBJECT));
        }
        Di++;
        if (Di >= Dj) {
            hasNext = false;
        }
        return result;
    }

    // --- Core Search Logic ---

    private void initSearch(IndexReader indexReader, long gId, long sId, long pId, long oId) {
        long id0 = getIdForComponent(positions[0], gId, sId, pId, oId);
        long id1 = getIdForComponent(positions[1], gId, sId, pId, oId);
        long id2 = getIdForComponent(positions[2], gId, sId, pId, oId);
        long id3 = getIdForComponent(positions[3], gId, sId, pId, oId);

        HDTBitmapDirectory d1 = indexReader.getDirectory(positions[1]);
        HDTBitmapDirectory d2 = indexReader.getDirectory(positions[2]);
        HDTBitmapDirectory d3 = indexReader.getDirectory(positions[3]);
        
        BitPackedUnSignedLongBuffer s1 = d1.getIds();
        BitPackedUnSignedLongBuffer s2 = d2.getIds();
        BitPackedUnSignedLongBuffer s3 = d3.getIds();

        long Ai, Aj, Bi, Bj, Ci, Cj;
        if (id0 != -1) {
            Ai = id0;
            Aj = id0;
        } else {
            Ai = 1;
            Aj = d1.rank1(d1.getNumBitmapEntries()); 
        }
        Node ss = dict.getObjects().extract(Ai);
        
        
        Bi = d1.select1(Ai);
        Bj = d1.select1(Ai + 1);
        if (Bi == -1) Bi = 0;
        if (Bj == -1) Bj = s1.getNumEntries();
        if (Bi >= Bj) {
            hasNext = false;
            return;
        }
        long ha = d1.getId(Bi+1);
        Node gg = dict.getGraphs().extract(d1.getId(Bi+1));
        
        if (id1 != -1) {
            long pos = findIdInRange(s1, id1, Bi, Bj);
            if (pos == -1) {
                hasNext = false;
                return;
            }
            Bi = pos;
            Bj = pos;
        }
        
        Ci = d2.select1( Bi + 1 );
        Cj = d2.select1( Bi + 2 );
        Node pp = dict.getPredicates().extract(d2.getId(Ci+1));        
        if (Ci == -1) Ci = 0;
        if (Cj == -1) Cj = s2.getNumEntries();
        if (Ci >= Cj) {
            hasNext = false;
            return;
        }
        
        if (id2 != -1) {
            long pos = findIdInRange(s2, id2, Ci, Cj);
            if (pos == -1) {
                hasNext = false;
                return;
            }
            Ci = pos;
            Cj = pos + 1;
        }
        
        Di = d3.select1( Ci );
        Dj = d3.select1( Cj + 1 );
        if (Di == -1) Di = 0;
        if (Dj == -1) Dj = s3.getNumEntries();
        if (Di >= Dj) { hasNext = false; return; }

        if (id3 != -1) {
            long pos = findIdInRange(s3, id3, Di, Dj);
            if (pos == -1) {
                hasNext = false;
                return;
            }
            Di = pos;
            Dj = pos + 1;
        }
        this.hasNext = true;
    }

    private QuadID extractQuadIdAtIndex(IndexReader indexReader, char[] positions, long tripleIdx) {
        long[] ids = new long[4];
        
        // Level 3 (Leaf)
        HDTBitmapDirectory d3 = indexReader.getDirectory(positions[3]);
        ids[getComponentIndex(positions[3])] = d3.getIds().get(tripleIdx);
        
        // Level 2
        long rank = d3.rank1(tripleIdx);
        HDTBitmapDirectory d2 = indexReader.getDirectory(positions[2]);
        ids[getComponentIndex(positions[2])] = d2.getIds().get(rank);
        
        // Level 1
        long level2Idx = rank;
        rank = d2.rank1(level2Idx); 
        HDTBitmapDirectory d1 = indexReader.getDirectory(positions[1]);
        ids[getComponentIndex(positions[1])] = d1.getIds().get(rank);
        
        // Level 0 (Implicit)
        long level1Idx = rank;
        rank = d1.rank1(level1Idx);
        ids[getComponentIndex(positions[0])] = rank; 

        return new QuadID(ids[0], ids[1], ids[2], ids[3]);
    }

    private long findIdInRange(BitPackedUnSignedLongBuffer buffer, long target, long start, long end) {
        long low = start;
        long high = end - 1;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            long midVal = buffer.get(mid);
            if (midVal < target) low = mid + 1;
            else if (midVal > target) high = mid - 1;
            else return mid;
        }
        return -1;
    }

    private record QuadID(long g, long s, long p, long o) {}

/**
     * Selects the best available index based on bound variables.
     * Checks the HDT Group to ensure the index actually exists.
     */
    private Index selectOptimalIndex(boolean g, boolean s, boolean p, boolean o) {
        // 1. All Bound: Try GSPO first, but any valid index will work fast.
        if (g && s && p && o) {
            return findFirstExisting(Index.GSPO, Index.SPOG, Index.POSG, Index.OSPG, Index.OPSG, Index.OGPS);
        }

        // 2. Object Bound (Most selective, usually requires specific O-indexes)
        if (o) {
            if (g) return findFirstExisting(Index.OGPS, Index.OSPG, Index.OPSG, Index.GSPO);
            if (p) return findFirstExisting(Index.OPSG, Index.POSG, Index.OSPG, Index.SPOG);
            if (s) return findFirstExisting(Index.OSPG, Index.SPOG, Index.GSPO);
            
            // Just Object bound: Prefer O-based, fallback to any containing O at reasonable depth
            return findFirstExisting(Index.OSPG, Index.OPSG, Index.OGPS, Index.POSG, Index.SPOG);
        }

        // 3. Graph Bound
        if (g) {
            if (s) return findFirstExisting(Index.GSPO, Index.SPOG);
            if (p) return findFirstExisting(Index.GPOS, Index.POSG, Index.GSPO);
            return findFirstExisting(Index.GSPO, Index.GOSP, Index.SPOG);
        }

        // 4. Subject Bound
        if (s) {
            if (p) return findFirstExisting(Index.SPOG, Index.POSG, Index.GSPO);
            return findFirstExisting(Index.SPOG, Index.GSPO);
        }

        // 5. Predicate Bound
        if (p) {
            return findFirstExisting(Index.POSG, Index.OPSG, Index.SPOG);
        }

        // 6. Fallback / Scan (Unbound)
        return findFirstExisting(Index.GSPO, Index.SPOG);
    }

    /**
     * Helper: Iterates through a list of preferred indexes and returns the first one
     * that exists in the HDT file structure.
     */
    private Index findFirstExisting(Index... candidates) {
        for (Index idx : candidates) {
            // Check if the Group (folder) exists in the HDF5 file for this index name
            if (hdt.getChild(idx.name()) != null) {
                return idx;
            }
        }
        return null; // Returns null if no optimized index is found (constructor handles fallback)
    }

    private IndexReader getIndexReader(Index indexType) {
        try {
            Group indexGroup = (Group) hdt.getChild(indexType.name());
            return (indexGroup == null) ? null : new IndexReader(indexGroup, indexType);
        } catch (Exception e) { return null; }
    }

    private long getIdForComponent(char component, long gId, long sId, long pId, long oId) {
        return switch (component) {
            case 'G' -> gId; case 'S' -> sId; case 'P' -> pId; case 'O' -> oId; default -> -1;
        };
    }
    
    private int getComponentIndex(char component) {
        return switch (component) {
            case 'G' -> 0; case 'S' -> 1; case 'P' -> 2; case 'O' -> 3; default -> -1;
        };
    }
}