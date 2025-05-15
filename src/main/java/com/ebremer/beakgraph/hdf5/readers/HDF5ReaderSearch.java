package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.QuadID;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import io.jhdf.api.Group;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.LongStream;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Search method for HDF5Reader that finds optimal index and returns QuadID iterator
 */
public class HDF5ReaderSearch {
    
    private final FiveSectionDictionaryReader dict;
    private final Group hdt;
    private final long totalQuads;
    private final IndexReader defaultReader;
    
    public HDF5ReaderSearch(int x, FiveSectionDictionaryReader dict, Group hdt, long totalQuads, IndexReader defaultReader) {
        this.dict = dict;
        this.hdt = hdt;
        this.totalQuads = totalQuads;
        this.defaultReader = defaultReader;
    } 
    
    public Iterator<QuadID> search(Quad quad) {
        boolean gBound = !quad.getGraph().isVariable();
        boolean sBound = !quad.getSubject().isVariable();
        boolean pBound = !quad.getPredicate().isVariable();
        boolean oBound = !quad.getObject().isVariable();
        
        Index optimalIndex = selectOptimalIndex(gBound, sBound, pBound, oBound);
        
        IndexReader selectedReader = getIndexReader(optimalIndex);
        if (selectedReader == null) {
            selectedReader = defaultReader; 
            optimalIndex = defaultReader.getIndexType();
        }
        
        long gId = gBound ? dict.getGraphs().locate(quad.getGraph()) : -1;
        long sId = sBound ? dict.getSubjects().locate(quad.getSubject()) : -1;
        long pId = pBound ? dict.getPredicates().locate(quad.getPredicate()) : -1;
        long oId = oBound ? dict.getObjects().locate(quad.getObject()) : -1;
        
        if ((gBound && gId == -1) || (sBound && sId == -1) || 
            (pBound && pId == -1) || (oBound && oId == -1)) {
            return new EmptyIterator<>();
        }
        
        return executeSearch(selectedReader, optimalIndex, gId, sId, pId, oId);
    }
    
    private Index selectOptimalIndex(boolean g, boolean s, boolean p, boolean o) {
        if (g && s && p && o) return Index.GSPO;
        if (o) {
            if (g) return Index.OGPS;
            if (p) return Index.OPSG;
            if (s) return Index.OSPG;
            return Index.OPSG;
        }
        if (g) {
            if (s) return Index.GSPO;
            if (p) return Index.GPOS;
            if (o) return Index.GOSP;
            return Index.GSPO;
        }
        if (s) {
            if (p) return Index.SPOG;
            return Index.SPOG;
        }
        if (p) {
            return Index.POSG;
        }
        return Index.GSPO;
    }
    
    private IndexReader getIndexReader(Index indexType) {
        try {
            Group indexGroup = (Group) hdt.getChild(indexType.name());
            if (indexGroup == null) {
                return null;
            }
            return new IndexReader(indexGroup, indexType);
        } catch (Exception e) {
            return null; // Index doesn't exist
        }
    }
    
    private Iterator<QuadID> executeSearch(IndexReader indexReader, Index indexType, long gId, long sId, long pId, long oId) {
        char[] positions = indexType.name().toCharArray();      
        long pos0Id = getIdForComponent(positions[0], gId, sId, pId, oId);
        if (pos0Id != -1) {
            return searchWithBoundFirstComponent(indexReader, positions, gId, sId, pId, oId, pos0Id);
        } else {
            return searchWithUnboundFirstComponent(indexReader, positions, gId, sId, pId, oId);
        }
    }
    
    /**
     * Search when the first component (position 0) is bound.
     */
    private Iterator<QuadID> searchWithBoundFirstComponent(IndexReader indexReader, char[] positions, long gId, long sId, long pId, long oId, long pos0Id) {
        long rank = pos0Id;
        HDTBitmapDirectory bitmapDir = indexReader.getDirectory(positions[1]);
        long startTriple = bitmapDir.select1(pos0Id);
        if (startTriple == -1) {
            return new EmptyIterator<>();
        }
        long numBitmapEntries = bitmapDir.getNumBitmapEntries();
        long totalRanks = bitmapDir.rank1(numBitmapEntries);
        long endTriple = (rank < totalRanks) ? bitmapDir.select1(rank + 1) : totalQuads;
        if (startTriple >= endTriple) {
            return new EmptyIterator<>();
        }        
        return LongStream
            .range(startTriple, endTriple)
            .mapToObj(tripleIdx -> extractQuadIdAtIndex(indexReader, positions, tripleIdx, pos0Id))
           // .filter(quadId -> matchesPattern(quadId, gId, sId, pId, oId))
            .iterator();
    }
    
    /**
     * Search when the first component (position 0) is unbound.
     */
    private Iterator<QuadID> searchWithUnboundFirstComponent(IndexReader indexReader, char[] positions, long gId, long sId, long pId, long oId) {
        return null;
        //return defaultReader.
    }
    
    /**
     * - dir(pos[1]) has B1 (bitmap for pos[0]) and S1 (ID/Rank list for pos[1])
     * - dir(pos[2]) has B2 (bitmap for pos[1]) and S2 (ID/Rank list for pos[2])
     * - dir(pos[3]) has B3 (bitmap for pos[2]) and S3 (full ID/Rank list for pos[3])
     */
    private QuadID extractQuadIdAtIndex(IndexReader indexReader, char[] positions, long tripleIdx, long pos0Id) {
        long[] ids = new long[4];
        long rank;
        ids[getComponentIndex(positions[3])] = indexReader.getDirectory(positions[3]).getIds().get(tripleIdx);
        rank = indexReader.getDirectory(positions[3]).rank1(tripleIdx);
        ids[getComponentIndex(positions[2])] = indexReader.getDirectory(positions[2]).getId(rank);
        rank = indexReader.getDirectory(positions[2]).rank1(tripleIdx);
        ids[getComponentIndex(positions[1])] = indexReader.getDirectory(positions[1]).getId(rank);
        if (pos0Id != -1) {
            ids[getComponentIndex(positions[0])] = pos0Id;
        } else {
            rank = indexReader.getDirectory(positions[1]).rank1(tripleIdx);
             ids[getComponentIndex(positions[0])] = rank;
        }
        Node gNode = dict.getGraphs().extract(ids[0]);
        Node sNode = dict.getSubjects().extract(ids[1]);
        Node pNode = dict.getPredicates().extract(ids[2]);
        Node oNode = dict.getObjects().extract(ids[3]);
        return new QuadID(ids[0], ids[1], ids[2], ids[3]);
    }

    /**
     * Gets the ID for a specific component character.
     */
    private long getIdForComponent(char component, long gId, long sId, long pId, long oId) {
        return switch (component) {
            case 'G' -> gId;
            case 'S' -> sId;
            case 'P' -> pId;
            case 'O' -> oId;
            default -> -1;
        };
    }
    
    /**
     * Gets the index (0-3) for a component character.
     */
    private int getComponentIndex(char component) {
        return switch (component) {
            case 'G' -> 0;
            case 'S' -> 1;
            case 'P' -> 2;
            case 'O' -> 3;
            default -> -1;
        };
    }
    
    /**
     * Checks if a QuadID matches the search pattern.
     */
    private boolean matchesPattern(QuadID quadId, long gId, long sId, long pId, long oId) {
        if (gId != -1 && quadId.g() != gId) return false;
        if (sId != -1 && quadId.s() != sId) return false;
        if (pId != -1 && quadId.p() != pId) return false;
        return !(oId != -1 && quadId.o() != oId);
    }
    
    /**
     * Empty iterator implementation.
     */
    private static class EmptyIterator<T> implements Iterator<T> {
        @Override
        public boolean hasNext() {
            return false;
        }
        
        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }
}
