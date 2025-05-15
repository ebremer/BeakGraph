package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface Dictionary {
    
    /**
     * Locates a Node and returns its ID.
     * @param element The Node to find.
     * @return The ID if found, or -1 if not found.
     */
    public long locate(Node element);

    /**
     * Searches for a Node and returns its ID or its insertion point.
     * <p>
     * This method follows the standard binary search contract:
     * <ul>
     * <li>If found: returns the positive ID.</li>
     * <li>If not found: returns {@code -(insertion_point) - 1}.</li>
     * </ul>
     * The {@code insertion_point} is the ID where the element would be inserted to maintain sort order.
     * This allows range queries (e.g. find values > X) to determine the correct starting ID even if X doesn't exist.
     * * @param element The Node to search for.
     * @param element
     * @return The ID or the encoded insertion point.
     */
    public long search(Node element);

    public Node extract(long id);
    public long getNumberOfNodes();
    public Stream<Node> streamNodes();
    
}