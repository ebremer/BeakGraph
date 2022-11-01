/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.solver;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;

/**
 *
 * @author erich
 */
public class ExecutionContextBeak extends ExecutionContext {
    private final Op op;
    
    public ExecutionContextBeak(ExecutionContext other, Op op) {
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
