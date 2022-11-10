package com.ebremer.beakgraph.rdf;

import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;

/**
 *
 * @author erich
 */
public class BGVoID {
    
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
