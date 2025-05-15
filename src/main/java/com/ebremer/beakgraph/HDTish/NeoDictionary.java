package com.ebremer.beakgraph.HDTish;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

/**
 *
 * @author Erich Bremer
 */
public class NeoDictionary {
    
    
    public static void main(String[] args) {
        LinkedHashSet<Node> namedgraphs = new LinkedHashSet<>();
        LinkedHashSet<Node> subjects = new LinkedHashSet<>();
        LinkedHashSet<Node> both = new LinkedHashSet<>();
        LinkedHashSet<Node> objects = new LinkedHashSet<>();
        LinkedHashSet<Node> predicates = new LinkedHashSet<>();

        Model m = ModelFactory.createDefaultModel();
        try (FileInputStream fis = new FileInputStream(new File("/tcga/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl"))) {
            RDFDataMgr.read(m, fis, Lang.TURTLE);
            System.out.println(m.size());
            ParameterizedSparqlString pss = new ParameterizedSparqlString("select ?s ?p ?o where {?s ?p ?o} order by ?s ?p ?o");
            QueryExecutionFactory.create(pss.toString(), m)
                .execSelect()
                .forEachRemaining(qs->{
                    Node s = qs.get("s").asResource().asNode();
                    Node p = qs.get("p").asResource().asNode();
                    Node o = qs.get("o").asNode();
                     //System.out.println(s+" "+p+" "+o);
                    //ByteBuffer buf;
                    predicates.add(p);
                    if (!both.contains(s)) {
                        if (objects.contains(s)) {
                            objects.remove(s);
                            both.add(s);
                        } else {
                            subjects.add(s);
                        }
                    }
                    if (!both.contains(o)) {
                        if (subjects.contains(o)) {
                            subjects.remove(o);
                            both.add(o);
                        } else {
                            objects.add(o);
                        }
                    }
                });
            System.out.println("Subjects   : " + subjects.size());
            System.out.println("Both       : " + both.size());
            System.out.println("Objects    : " + objects.size());            
            System.out.println("Predicates : " + predicates.size());
            int num = subjects.size() + predicates.size() + objects.size() + both.size();
            System.out.println("Total      : " + num);
            objects.stream()
                .filter(n->!n.toString().startsWith("\"POLYGON"))
                .forEach(n->System.out.println(n));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(NeoDictionary.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(NeoDictionary.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
