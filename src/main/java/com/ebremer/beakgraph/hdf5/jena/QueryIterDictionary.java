package com.ebremer.beakgraph.hdf5.jena;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.jena.sparql.engine.ExecutionContext;

public class QueryIterDictionary extends QueryIterPlainWrapper {
    public QueryIterDictionary(Var var, Stream<Node> nodes, ExecutionContext execCxt) {
        super(convert(var, nodes.iterator()), execCxt);
    }

    private static Iterator<Binding> convert(Var var, Iterator<Node> nodes) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return nodes.hasNext(); }
            @Override public Binding next() {
                return BindingFactory.binding(var, nodes.next());
            }
        };
    }
}