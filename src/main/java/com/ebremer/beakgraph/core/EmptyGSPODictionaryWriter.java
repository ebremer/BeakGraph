package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.core.GSPODictionary;
import io.jhdf.api.WritableGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public class EmptyGSPODictionaryWriter implements DictionaryWriter, GSPODictionary {

    @Override
    public long locateGraph(Node element) {
        return -1;
    }

    @Override
    public Object extractGraph(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locateSubject(Node element) {
        return -1;
    }

    @Override
    public Object extractSubject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locatePredicate(Node element) {
        return -1;
    }

    @Override
    public Object extractPredicate(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locateObject(Node element) {
        return -1;
    }

    @Override
    public Object extractObject(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

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
    public Stream<Node> streamSubjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamPredicates() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamObjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<Node> streamGraphs() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
