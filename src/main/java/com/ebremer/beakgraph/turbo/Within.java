package com.ebremer.beakgraph.turbo;

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.pfunction.PFuncSimple;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class Within extends PFuncSimple {

    @Override
    public QueryIterator execEvaluated(Binding binding, Node subject, Node predicate, Node object, ExecutionContext execCxt) {    
        Model m = ModelFactory.createModelForGraph(execCxt.getActiveGraph());      
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select *           
            where {
                ?s geo:hasGeometry ?geometry .
                ?geometry hal:asHilbert/hal:hasRange ?range .
                ?range hal:low ?low; hal:high ?high
            }
            """
        );
        pss.setParam("s", subject);
        pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),m);
        ResultSet rs = qe.execSelect();
        ResultSetFormatter.out(System.out, rs);
        
        
        List<Binding> results = new ArrayList<>();
        results.add(BindingFactory.builder(binding).build());
        return QueryIterPlainWrapper.create(results.iterator(), execCxt);
    }
    
}
