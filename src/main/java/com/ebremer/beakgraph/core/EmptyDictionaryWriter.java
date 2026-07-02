package com.ebremer.beakgraph.core;

import io.jhdf.api.WritableGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 * Null-object dictionary for a section with no entries (e.g. a source with no
 * literals). Every operation answers honestly for an empty dictionary instead
 * of throwing template "Not supported yet." exceptions.
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
    public void add(WritableGroup group) {}

    @Override
    public long locate(Node element) {
        return -1;
    }

    @Override
    public Node extract(long id) {
        throw new IllegalArgumentException("id [" + id + "]: empty dictionary has no entries");
    }

    @Override
    public Stream<Node> streamNodes() {
        return Stream.empty();
    }

    @Override
    public long search(Node element) {
        // Nothing matches; the insertion point in an empty 1-based dictionary is 1,
        // encoded -(insertion)-1 per the search contract.
        return -2;
    }
}
