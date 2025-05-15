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
    public void Add( WritableGroup group );
}
