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
	QC.setFactory(ARQ.getContext(), OpExecutorBeak.opExecFactoryBeak);
	QueryEngineBeak.register();
    }

    private final BeakReader reader;

    public BeakGraph(URI uri) throws IOException {
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
        return new PredicateIterator(reader);
        //throw new UnsupportedOperationException("graphBaseFind Not supported yet.");
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

    public ReorderTransformation getReorderTransform() {
        return null;
    }
    
    /*
    public static void main(String[] args) throws IOException {
        //JenaSystem.init();
        //File f = new File("D:\\HalcyonStorage\\heatmaps\\j3.zip");
        File f = new File("D:\\HalcyonStorage\\segmentation\\zzz.zip");
        URI uri = f.toURI();
        BeakGraph g = new BeakGraph(uri);
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select * {
            #select distinct ?polygon ?low ?high ?class ?certainty where {
                {
                    select * {
                    #select ?polygon ?low ?high ?class ?certainty where {
                        ?range hal:low ?low .
                        ?range hal:high ?high .
                        ?polygon ?p ?range .
                        ?annotation oa:hasSelector ?polygon .
                        ?annotation oa:hasBody ?body .
                        ?body hal:assertedClass ?class .
                        ?body hal:hasCertainty ?certainty .
                        filter(?low>=?rlow)
                        filter(?low<=?rhigh)
                    }
                } union {
                    select * {
                    #select ?polygon ?low ?high ?class ?certainty where {
                        ?range hal:low ?low .
                        ?range hal:high ?high .
                        ?polygon ?p ?range .
                        ?annotation oa:hasSelector ?polygon .
                        ?annotation oa:hasBody ?body .
                        ?body hal:assertedClass ?class .
                        ?body hal:hasCertainty ?certainty .
                        filter(?high>=?rlow)
                        filter(?high<=?rhigh)
                    }
                }
            } limit 30
            """);
        pss.setNsPrefix("so", SchemaDO.NS);
        pss.setIri("p", "https://www.ebremer.com/halcyon/ns/"+"hasRange/"+1);
        pss.setNsPrefix("oa", OA.NS);
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        pss.setLiteral("rlow", 0);
        pss.setLiteral("rhigh", Long.MAX_VALUE);
        Model m = ModelFactory.createModelForGraph(g);
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(), m);
        ResultSet rs = qe.execSelect();
        ResultSetFormatter.out(System.out,rs);
    }*/
}
