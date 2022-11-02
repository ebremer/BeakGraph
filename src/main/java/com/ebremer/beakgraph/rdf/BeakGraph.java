package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.solver.OpExecutorBeak;
import com.ebremer.beakgraph.solver.QueryEngineBeak;
import com.ebremer.beakgraph.solver.StageGeneratorDirectorBeak;
import com.ebremer.rocrate4j.ROCrate;
import com.ebremer.rocrate4j.ROCrateReader;
import com.ebremer.rocrate4j.readers.ZipReader;
import com.ebremer.rocrate4j.writers.ZipWriter;
import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OA;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;

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

    public BeakGraph(ROCrateReader rocreader) throws IOException {
        this.reader = new BeakReader(rocreader);
        wireIntoExecution();
    }
    
    public BeakReader getReader() {
        return reader;
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
    
    private static void wireIntoExecution() {
        Context cxt = ARQ.getContext() ;
        StageGenerator orig = StageBuilder.chooseStageGenerator(cxt) ;
        StageGenerator stageGenerator = new StageGeneratorDirectorBeak(orig) ;
        StageBuilder.setGenerator(ARQ.getContext(), stageGenerator) ;
    }
    
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
        */
        
        /*
        """
            select * where {
                ?CreateAction a so:CreateAction;
                so:object/exif:width ?width;
                so:object/exif:height ?height
            } limit 1
            """); 
        */
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
                """
                select * where {
                    
                }
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

    public ReorderTransformation getReorderTransform() {
        return null;
    }
       
    public static void main(String[] args) throws IOException {
        System.out.println("Loading RDF...");
        /*
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new GZIPInputStream(new FileInputStream("/nlms2/halcyon/TCGA-3C-AALI-01Z-00-DX1.F6E9A5DF-D8FB-45CF-B4BD-C6B76294C291.ttl.gz")), Lang.TURTLE);
        //ROCrate.Builder builder = new ROCrate.Builder(new ZipWriter(new File("d:\\nlms2\\halcyon\\x.zip")));
        ROCrate.Builder builder = new ROCrate.Builder(new FolderWriter(new File("d:\\nlms2\\halcyon\\x")));
        new BeakWriter(m, builder, "halcyon");
        builder.build();
        */
        String base = "http://www.ebremer.com/YAY";
        BeakGraph g = new BeakGraph(new ROCrateReader(base, new ZipReader(new File("d:\\nlms2\\halcyon\\x.zip"))));
        g.Core();
    }    
}
