package com.ebremer.beakgraph.rdf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class TestRead {
    
    public static void main(String[] args) throws IOException, URISyntaxException {
        JenaSystem.init();
        //ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        //root.setLevel(ch.qos.logback.classic.Level.OFF);
        //URI uri = new URI("file:///D:/HalcyonStorage/nuclearsegmentation2019/coad/TCGA-AA-3872-01Z-00-DX1.eb3732ee-40e3-4ff0-a42b-d6a85cfbab6a.zip");
        URI uri = new URI("file:///D:/HalcyonStorage/nuclearsegmentation2019/coad/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.zip");
        BeakGraph g = new BeakGraph(uri);
        Model m = ModelFactory.createModelForGraph(g);
        System.out.println("SIZE : "+m.size());
        //RDFDataMgr.write(System.out, m, Lang.TURTLE);
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select * where {?s a ?o} limit 30         
            """);
        pss.setNsPrefix("rdf", RDF.getURI());
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        Query query = QueryFactory.create(pss.toString());
        try (QueryExecution qexec = QueryExecutionFactory.create(query, m)) {
            ResultSet results = qexec.execSelect();
            while (results.hasNext()) {
                QuerySolution qs = results.next();
                System.out.println(qs.get("s"));
            }
            //ResultSetFormatter.out(System.out, results, query);
        }
    }
    
}
