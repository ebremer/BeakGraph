package com.ebremer.beakgraph.ng;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;

/**
 *
 * @author erich
 */
public class QueryEngineBeak extends QueryEngineMain {   
    protected static final QueryEngineFactory factory = new TQueryEngineFactory() ;
    static public QueryEngineFactory getFactory() { return factory; } 
    static public void register(){ QueryEngineRegistry.addFactory(factory); }
    static public void unregister(){ QueryEngineRegistry.removeFactory(factory); }
    
    public QueryEngineBeak(Query query, DatasetGraph dataset, Binding input, Context context) { 
        super(query, dataset, input, context);
    }
	
    public QueryEngineBeak(Op op, DatasetGraph dataset, Binding input, Context context) {
	super(op, dataset, input, context);
    }

    protected static class TQueryEngineFactory implements QueryEngineFactory {
        
        private static boolean isHandledByBG(DatasetGraph dataset) {
            return false ;
        }
        
    	@Override
    	public boolean accept(Query query, DatasetGraph dataset, Context context) {   	
            return isHandledByBG(dataset);
    	}

        @Override
    	public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return isHandledByBG(dataset);
    	}
        
    	@Override
    	public Plan create(Query query, DatasetGraph dataset, Binding initial, Context context) {
            QueryEngineBeak engine = new QueryEngineBeak(query, dataset, initial, context);
            return engine.getPlan();
    	}

    	@Override
    	public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            throw new ARQInternalErrorException("TQueryEngine: factory called directly with an algebra expression");
    	}
    }
}
