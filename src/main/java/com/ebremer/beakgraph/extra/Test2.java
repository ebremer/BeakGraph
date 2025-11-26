package com.ebremer.beakgraph.extra;

import com.ebremer.beakgraph.ng.BG;
import com.ebremer.beakgraph.ng.BGDatasetGraph;
import com.ebremer.beakgraph.ng.BeakGraph;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class Test2 {

    /**
     * @param args the command line arguments
     * @throws java.io.FileNotFoundException
     */
    public static void main(String args[]) throws FileNotFoundException, IOException {        
        File file = new File("D:\\remotehdt\\1-lubm.ttl");
        File dest = new File("D:\\remotehdt\\1-lubm.zip");        
        if (!dest.exists()) {
            Model m = ModelFactory.createDefaultModel();
            RDFDataMgr.read(m, new FileInputStream(file), Lang.TURTLE);
            System.out.println("# of triples loaded : "+m.size());
            Dataset ds = DatasetFactory.create();
            ds.getDefaultModel().add(m);
            BG.getBuilder()
                .dataset(ds)
                .file(dest)
                .build();
        }
        
        BeakGraph bg = new BeakGraph(dest.toURI());
        Dataset ds = DatasetFactory.wrap(new BGDatasetGraph(bg));
        
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select *
            where {
                ?s ?p ?o
            }
            """
        );
        pss.setNsPrefix("rdf", RDF.getURI());
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),ds);
        ResultSetFormatter.out(System.out, qe.execSelect());
        
    }
}
