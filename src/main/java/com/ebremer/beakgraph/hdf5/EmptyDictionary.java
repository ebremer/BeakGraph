package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.core.AbstractDictionary;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public class EmptyDictionary extends AbstractDictionary {

    @Override
    public long locate(Node element) {
        return -1;
    }

    @Override
    public Node extract(long id) {
        return null;
    }

    @Override
    public Stream<Node> streamNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getNumberOfNodes() {
        return 0;
    }

    @Override
    public long search(Node element) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
