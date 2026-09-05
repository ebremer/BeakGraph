package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 * The READ-side dictionary contract, what {@code BGReader.getDictionary()}
 * returns: the per-position dictionaries plus the role streams. The writers
 * do not implement it (they used to, with fourteen throwing stubs, BG-311).
 *
 * @author Erich Bremer
 */
public interface GSPODictionary {

    public Dictionary getGraphs();
    public Dictionary getSubjects();
    public Dictionary getPredicates();
    public Dictionary getObjects();

    public long locateGraph(Node element);
    public long locateSubject(Node element);
    public long locatePredicate(Node element);
    public long locateObject(Node element);

    /**
     * Number of distinct terms that occur as a subject (the columnar
     * {@code subjects} list, SPECIFICATIONS §7.8), or -1 when unknown. The
     * subject dictionary's size is the whole entity section - graph names
     * and object-only entities included - which the reorder cost model used
     * to divide by (BG-12).
     */
    public default long subjectCount() { return -1; }

    /** Number of distinct terms that occur as an object (the columnar {@code objects} list), or -1 when unknown. */
    public default long objectCount() { return -1; }

    
    public Stream<Node> streamGraphs();
    public Stream<Node> streamSubjects();
    public Stream<Node> streamPredicates();
    public Stream<Node> streamObjects();
    
}
