package com.ebremer.beakgraph.ng;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;

/**
 *
 * @author erich
 */
public class ExecutionContextBG extends ExecutionContext {
    private final Op op;
    
    public ExecutionContextBG(ExecutionContext other, Op op) {
        super(other);
        this.op = op;
    }
    
    public ExprList getFilter() {
        if (op instanceof OpFilter opFilter) {
            return opFilter.getExprs();
        }
        return null;
    }
}
