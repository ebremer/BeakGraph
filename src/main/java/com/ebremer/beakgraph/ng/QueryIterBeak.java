package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import java.util.List;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;

/**
 *
 * @author erich
 */
public class QueryIterBeak extends QueryIterPlainWrapper {
    final private QueryIterator originalInput;
    private final List<Abortable> killList;
    
    public QueryIterBeak(Iterator<Binding> iterBinding, List<Abortable> killList, QueryIterator originalInput, ExecutionContext execCxt) {
        super(iterBinding, execCxt);
        this.originalInput = originalInput;
        this.killList = killList;
    }
    
    @Override
    protected void closeIterator() { 
        if (originalInput!=null)
            originalInput.close();
        super.closeIterator();
    }

    @Override
    protected void requestCancel() { 
        if (killList!=null)
            for ( Abortable it : killList )
                it.abort();
    } 
}
