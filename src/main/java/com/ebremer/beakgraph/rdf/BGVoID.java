package com.ebremer.beakgraph.rdf;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;

/**
 *
 * @author erich
 */
public class BGVoID {
    private long numtriples = 0;
    private long numclasses = 0;
    //private final HashSet<String> triples = new HashSet<>();
    private final ConcurrentHashMap<String,Long> predicatecounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,Long> classcounts = new ConcurrentHashMap<>();
    
    public BGVoID() {
        
    }
    
    public void Add(Statement stmt) {
        CountClass(stmt);
        CountTriple(stmt);
        CountPredicate(stmt);
    }
    
    public void CountClass(Statement stmt) {
        if (stmt.getPredicate()==RDF.type) {
            numclasses++;
        }
    }
    
    public void CountPredicate(Statement stmt) {
        String p = stmt.getPredicate().toString();
        if (!predicatecounts.containsKey(p)) {
            predicatecounts.put(p, 1L);
        } else {
            predicatecounts.put(p, predicatecounts.get(p)+1L);
        }
    }
    
    public void CountTriple(Statement stmt) {
        numtriples++;
        /*
        StringBuilder sb = new StringBuilder();
        sb.append(stmt.getSubject().toString())
          .append("/")
          .append(stmt.getPredicate().toString())
          .append("/")
          .append(stmt.getObject().toString());
        String hash = DigestUtils.md5Hex(sb.toString());
        if (!triples.contains(hash)) {
            triples.add(hash);
            numtriples++;
        }
*/
    }
    
    public Model getVoid(Resource ng) {
        Model m = ModelFactory.createDefaultModel();
        predicatecounts.forEach((k,v)->{
            ng.addProperty(
                VOID.propertyPartition,
                m.createResource()
                    .addProperty(VOID.property, m.createResource(k))
                    .addLiteral(VOID.triples, v)
            );
        });
        classcounts.forEach((k,v)->{
            ng.addProperty(VOID.propertyPartition,
                m.createResource()
                    .addProperty(VOID._class, m.createResource(k))
                    .addLiteral(VOID.triples, v)
            );
        });
        Resource root = m.createResource(ng.getURI()+"/void");
        root
            .addProperty(RDF.type, VOID.Dataset)
            .addProperty(VOID.dataDump, ng)
            .addLiteral(VOID.triples, numtriples);
        return m;
    }
    
    public static Model GenerateVoID(Resource ng, Model m) {
        Model VoID = ModelFactory.createDefaultModel();
        Resource root = VoID.createResource(ng.getURI()+"/void");
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            construct {
                ?ng void:propertyPartition [
                    void:property ?p;
                    void:triples ?num
                ]
            } where {
                {
                    select ?p (count(*) as ?num)
                    where {
                        ?s ?p ?o
                    }
                    group by ?p
                }
            }
            """);
        pss.setIri("ng", ng.getURI());
        pss.setNsPrefix("void", VOID.NS);
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(), m);
        VoID.add(qe.execConstruct());
        pss = new ParameterizedSparqlString(
            """
            construct {
                ?ng void:classPartition [
                    void:class ?p;
                    void:triples ?num
                ]
            } where {
                {
                    select ?o (count(*) as ?num)
                    where {
                        ?s a ?o
                    }
                    group by ?o
                }
            }
            """);
        pss.setIri("ng", ng.getURI());
        pss.setNsPrefix("void", VOID.NS);
        qe = QueryExecutionFactory.create(pss.toString(), m);
        VoID.add(qe.execConstruct());
        pss = new ParameterizedSparqlString(
            """
            construct {
                ?root void:properties ?num
            } where {
                {
                    select (count(distinct ?p) as ?num)
                    where {
                        ?s ?p ?o
                    }
                }
            }
            """);
        pss.setIri("ng", ng.getURI());
        pss.setNsPrefix("void", VOID.NS);
        qe = QueryExecutionFactory.create(pss.toString(), m);
        VoID.add(qe.execConstruct());
        root
            .addProperty(RDF.type, VOID.Dataset)
            .addProperty(VOID.dataDump, ng)
            .addLiteral(VOID.triples, m.size());
        return VoID;
    }
}
