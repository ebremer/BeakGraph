package com.ebremer.beakgraph.ng;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 * @param <Triple>
 */
public class PredicateIterator<Triple> implements ExtendedIterator<Triple> {
    private final HashMap<String, PAR> pred;
    private final NodeTable nt;
    private final IteratorChain<Triple> ic;
    private static final Logger logger = LoggerFactory.getLogger(PredicateIterator.class);
    
    public PredicateIterator(int ng, BeakReader reader, Triple triple) {
        logger.trace(triple.toString());
        this.nt = reader.getNodeTable();
        this.pred = reader.getPredicates();
        ArrayList<Iterator<Triple>> list = new ArrayList<>();
        pred.keySet().iterator().forEachRemaining(s->{        
            list.add(new PARIterator(ng, pred.get(s), nt));
        });
        ic = new IteratorChain(list);
    }
    
    @Override
    public boolean hasNext() {
        return ic.hasNext();
    }

    @Override
    public Triple next() {
        return ic.next();
    }


    @Override
    public ExtendedIterator andThen(Iterator itrtr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ExtendedIterator filterKeep(Predicate prdct) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ExtendedIterator filterDrop(Predicate prdct) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List toList() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set toSet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {}

    @Override
    public Triple removeNext() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <U> ExtendedIterator<U> mapWith(Function<Triple, U> function) {
        Iterator<U> mappedIterator = new Iterator<U>() {
            @Override
            public boolean hasNext() {
                return PredicateIterator.this.hasNext();
            }

            @Override
            public U next() {
                Triple t = PredicateIterator.this.next();
                return function.apply(t);
            }
        };
        return WrappedIterator.create(mappedIterator);
    }
}
