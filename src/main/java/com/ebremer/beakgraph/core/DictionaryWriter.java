package com.ebremer.beakgraph.core;

import io.jhdf.api.WritableGroup;
import java.util.List;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface DictionaryWriter {
    public long getNumberOfNodes();
    public List<Node> getNodes();
    public void add( WritableGroup group );

    /** The id of {@code element} in this (complete) section, or -1 when absent - what the index writers resolve through. */
    public long locate(Node element);
}
