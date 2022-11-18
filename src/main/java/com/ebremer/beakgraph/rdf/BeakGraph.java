package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.solver.OpExecutorBeak;
import com.ebremer.beakgraph.solver.QueryEngineBeak;
import com.ebremer.beakgraph.solver.StageGeneratorDirectorBeak;
import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 *
 * @author erich
 */
public class BeakGraph extends GraphBase {
    
    static {
	QC.setFactory(ARQ.getContext(), OpExecutorBeak.opExecFactoryRaptor);
	QueryEngineBeak.register();
    }

    private final BeakReader reader;

    //public BeakGraph(String base, URI uri) throws IOException {
    public BeakGraph(URI uri) throws IOException {
        //this.reader = new BeakReader(base, uri);
        this.reader = new BeakReader(uri);
        wireIntoExecution();
    }
    
    public BeakReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        reader.close();
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple tp) {
        System.out.println("---> graphBaseFind( "+tp+" )");
        throw new UnsupportedOperationException("graphBaseFind Not supported yet.");
    }

    @Override
    public void add(Node s, Node p, Node o) throws AddDeniedException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void delete(Node s, Node p, Node o) throws DeleteDeniedException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Stream<Triple> stream(Node s, Node p, Node o) {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Stream<Triple> stream() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public ExtendedIterator<Triple> find() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }
    
    @Override
    protected int graphBaseSize() {
        return reader.getNumberOfTriples();
    }
    
    private static void wireIntoExecution() {
        Context cxt = ARQ.getContext() ;
        StageGenerator orig = StageBuilder.chooseStageGenerator(cxt) ;
        StageGenerator stageGenerator = new StageGeneratorDirectorBeak(orig) ;
        StageBuilder.setGenerator(ARQ.getContext(), stageGenerator) ;
    }
    /*
    public void Core() {    
        Model m = ModelFactory.createModelForGraph(this);
        /*
              #filter(isiri(?type))
              #values (?type) {(<http://www.ebremer.com/1>) (<http://www.ebremer.com/2>) (<http://www.ebremer.com/3>)}
            """); */
        /*
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select * where {
                #?x rdf:value ?polygon
                ?s  oa:hasBody [
                        hal:hasCertainty ?certainty;
                        a ?BodyType;
                        hal:assertedClass ?assertedClass
                    ];
                    oa:hasSelector [
                        a ?SelectorType
                        #rdf:value ?polygon
                    ]
                #filter(?certainty>0.0)
                #filter(?certainty<1.0)
            } limit 10
            """); 
        
        
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select * where {
                ?s oa:hasBody ?body
            } limit 20
            """);
        pss.setNsPrefix("", "https://www.ebremer.com/ns/");
        pss.setNsPrefix("rdf", RDF.uri);
        pss.setNsPrefix("rdfs", RDFS.uri);
        pss.setNsPrefix("so", SchemaDO.NS);
        pss.setNsPrefix("oa", OA.NS);
        pss.setNsPrefix("exif", "http://www.w3.org/2003/12/exif/ns#");
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        pss.setNsPrefix("dcmi", "http://purl.org/dc/terms/");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(), m);
        ResultSet results = qe.execSelect();
        System.out.println("======================= DONE ============================= ");
        ResultSetFormatter.out(System.out, results);
    }
    
    public void Core2(String sparql) {    
        long begin = System.nanoTime();
        Model m = ModelFactory.createModelForGraph(this);
        QueryExecution qe = QueryExecutionFactory.create(sparql, m);
        ResultSet results = qe.execSelect();
        ResultSetFormatter.out(System.out, results);
        System.out.println("Lapse : "+(System.nanoTime()-begin));
    }
*/
    public ReorderTransformation getReorderTransform() {
        return null;
    }
}
