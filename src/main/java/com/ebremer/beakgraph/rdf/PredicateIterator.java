/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.rdf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 *
 * @author erich
 */
public class PredicateIterator implements ExtendedIterator {
    private final HashMap<String, PAR> pred;
    private final Iterator<String> key;
    private Iterator<Triple> values;
    private NodeTable nt;
    
    public PredicateIterator(BeakReader reader) {
        this.nt = reader.getNodeTable();
        this.pred = reader.getPredicates();
        this.key = pred.keySet().iterator();
        this.values = new PARIterator(pred.get(key.next()),nt);
    }
    
    @Override
    public boolean hasNext() {
        return key.hasNext()||values.hasNext();
    }

    @Override
    public Triple next() {
        Triple triple = values.next();
        if (!values.hasNext()) {
            if (key.hasNext()) {
               values = new PARIterator(pred.get(key.next()),nt); 
            }
        }
        return triple;
    }

    @Override
    public Object removeNext() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public ExtendedIterator andThen(Iterator itrtr) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public ExtendedIterator filterKeep(Predicate prdct) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public ExtendedIterator filterDrop(Predicate prdct) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public ExtendedIterator mapWith(Function fnctn) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public List toList() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Set toSet() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
}
