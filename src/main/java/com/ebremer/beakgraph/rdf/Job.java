/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.rdf;

import java.util.concurrent.Callable;
import org.apache.jena.rdf.model.Model;

/**
 *
 * @author erich
 */
public class Job {
    
    public String predicate;
    public Callable<Model> worker;
    public String status;
    
    public Job(String predicate, Callable<Model> worker, String status) {
        this.predicate = predicate;
        this.worker = worker;
        this.status = status;
    }
}
