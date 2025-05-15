package com.ebremer.beakgraph.ng;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;

/**
 *
 * @author erich
 */
public class BGVoID {
    private final ConcurrentHashMap<Resource,Long> PredicatesInstanceCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Resource,HashSet<Resource>> classInstanceCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Resource,Long> distinctObjects = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Resource,Long> distinctSubjects = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Resource,Long> classCounts = new ConcurrentHashMap<>();
    private long numtriples = 0;
    
    public BGVoID() {
    }
    
    public void Add(Statement stmt) {
        CountTriple(stmt);
        CountPredicate(stmt);
        CountSubject(stmt);
        CountObject(stmt);
        CountClass(stmt);
        CountClassInstances(stmt);
    }
    
    public void CountTriple(Statement stmt) {
        numtriples++;
    }
    
    public void CountPredicate(Statement stmt) {
        Resource p = stmt.getPredicate();
        if (!PredicatesInstanceCounts.containsKey(p)) {
            PredicatesInstanceCounts.put(p, 1L);
        } else {
            PredicatesInstanceCounts.put(p, PredicatesInstanceCounts.get(p)+1L);
        }
    }
    
    public void CountSubject(Statement stmt) {
        Resource subject = stmt.getSubject();
        if (!distinctSubjects.containsKey(subject)) {
            distinctSubjects.put(subject, 1L);
        } else {
            distinctSubjects.put(subject, distinctSubjects.get(subject)+1L);
        }
    }
    
    public void CountObject(Statement stmt) {
        RDFNode rdfnode = stmt.getObject();
        if (rdfnode.isResource()) {
            Resource object = rdfnode.asResource();
            if (!distinctObjects.containsKey(object)) {
               distinctObjects.put(object, 1L);
            } else {
                distinctObjects.put(object, distinctObjects.get(object)+1L);
            }
        }
    }
    
    public void CountClass(Statement stmt) {        
        if (stmt.getPredicate().equals(RDF.type)) {
            Resource clazz = stmt.getObject().asResource();
            if (!classCounts.containsKey(clazz)) {
                classCounts.put(clazz, 1L);
            } else {
                classCounts.put(clazz, classCounts.get(clazz)+1L);
            }
        }
    }
    
    public void CountClassInstances(Statement stmt) {        
        if (stmt.getPredicate().equals(RDF.type)) {
            Resource clazz = stmt.getObject().asResource();
            if (!classInstanceCounts.containsKey(clazz)) {
                classInstanceCounts.put(clazz, new HashSet<>());
            }
            classInstanceCounts.get(clazz).add(stmt.getSubject());         
        }
    }
    
    public void generateVoid(Resource ng) {
        Model m = ng.getModel();
        ng
            .addProperty(RDF.type, VOID.Dataset)
            .addLiteral(VOID.classes, classInstanceCounts.size())
            .addLiteral(VOID.properties, PredicatesInstanceCounts.size())
            .addLiteral(VOID.distinctSubjects, distinctSubjects.size())
            .addLiteral(VOID.distinctObjects, distinctObjects.size())
            .addLiteral(VOID.triples, numtriples);
        PredicatesInstanceCounts.forEach((k,v)->{
            ng.addProperty(
                VOID.propertyPartition,
                m.createResource()
                    .addProperty(VOID.property, k)
                    .addLiteral(VOID.triples, v)
            );
        });
        classInstanceCounts.forEach((k,v)->{
            ng.addProperty(VOID.classPartition,
                m.createResource()
                    .addProperty(VOID._class, k)
                    .addLiteral(VOID.entities, v.size())
            );
        });
    }
}
