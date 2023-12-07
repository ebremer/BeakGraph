package com.ebremer.beakgraph.turbo;

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
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
        System.out.println("YAY =============================");        
        Model m = ModelFactory.createModelForGraph(execCxt.getActiveGraph());      
        Resource s = m.asRDFNode(subject).asResource();
        //s.listProperties().forEach(st->{
          //  System.out.println("===> "+st);
        //});
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select *           
            where {
                ?s ?p ?o
            }
            """
        );
        pss.setIri("s", subject.toString());
        System.out.println("THIS ==>\n"+pss.toString());
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),m);
        ResultSet rs = qe.execSelect();
        rs.forEachRemaining(qs->{
            System.out.println("KHAN ==> "+qs.get("p"));
        });
        
        
        List<Binding> results = new ArrayList<>();
        results.add(BindingFactory.builder(binding).build());
        return QueryIterPlainWrapper.create(results.iterator(), execCxt);
    }
    
}
