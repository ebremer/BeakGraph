package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.core.AbstractDictionary;
import io.jhdf.api.WritableGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public class EmptyDictionaryWriter extends AbstractDictionary implements DictionaryWriter {

    @Override
    public long getNumberOfNodes() {
        return 0;
    }

    @Override
    public List<Node> getNodes() {
        return new ArrayList<>();
    }

    @Override
    public void Add(WritableGroup group) {}

    @Override
    public long locate(Node element) {
        return -1;
    }

    @Override
    public Node extract(long id) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Stream<Node> streamNodes() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public long search(Node element) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
