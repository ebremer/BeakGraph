package com.ebremer.beakgraph.hdf5.jena;

import org.apache.jena.graph.*;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.iterator.ExtendedIterator;

public class BNodeTranslatingGraph extends GraphWrapper {

    private final NodeTransform storageToJena;
    private final NodeTransform jenaToStorage;

    public BNodeTranslatingGraph(Graph baseGraph,
                                 NodeTransform storageToJena,
                                 NodeTransform jenaToStorage) {
        super(baseGraph);
        this.storageToJena = storageToJena;
        this.jenaToStorage = jenaToStorage;
    }

    @Override
    public ExtendedIterator<Triple> find(Triple match) {
        Triple matchInStorage = NodeTransformLib.transform(jenaToStorage, match);
        return mappedIterator(get().find(matchInStorage));
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        Node s2 = s.isBlank() ? jenaToStorage.apply(s) : s;
        Node p2 = p.isBlank() ? jenaToStorage.apply(p) : p;
        Node o2 = o.isBlank() ? jenaToStorage.apply(o) : o;
        return mappedIterator(get().find(s2, p2, o2));
    }

    /**
     * Corrected implementation using Jena's native mapWith.
     * This replaces the 40+ line anonymous inner class.
     */
    private ExtendedIterator<Triple> mappedIterator(ExtendedIterator<Triple> baseIt) {
        // Use NodeTransformLib to transform the whole triple at once, 
        // matching the logic used elsewhere in your class.
        return baseIt.mapWith(t -> NodeTransformLib.transform(storageToJena, t));
    }

    @Override public void add(Triple t)     { get().add(NodeTransformLib.transform(jenaToStorage, t)); }
    @Override public void delete(Triple t)  { get().delete(NodeTransformLib.transform(jenaToStorage, t)); }
    @Override public boolean contains(Triple t) { return get().contains(NodeTransformLib.transform(jenaToStorage, t)); }
    @Override public int size()              { return get().size(); }
    @Override public void clear()            { get().clear(); }
    @Override public PrefixMapping getPrefixMapping() { return get().getPrefixMapping(); }
    @Override public boolean isEmpty()       { return get().isEmpty(); }
}