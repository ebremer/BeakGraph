package com.ebremer.beakgraph.ng;

import com.ebremer.beakgraph.control.AllocatorCore;
import static com.ebremer.beakgraph.ng.BeakGraph.DICTIONARY;
import static com.ebremer.beakgraph.ng.BeakGraph.NAMEDGRAPHS;
import com.ebremer.rocrate4j.ROCrateReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;

/**
 *
 * @author erich
 */
public final class BeakReader implements AutoCloseable {
    private final NodeTable nodeTable;
    private final HashMap<String,PAR> byPredicate;
    private final Model manifest;
    private int numtriples = 0;
    private final ROCrateReader reader;
    private final URI uri;
    private final URI base;
    private Mapper mapper = null;
    private boolean usemapper;
    
    public BeakReader(URI uri) throws FileNotFoundException, IOException {
        this(uri, uri);
    }
    
    public BeakReader(URI uri, URI base) throws FileNotFoundException, IOException {
        StopWatch sw = StopWatch.getInstance();
        this.uri = uri;
        this.base = base;        
        this.usemapper = !uri.equals(base);
        if (usemapper) {
            this.mapper = new Mapper(uri, base);
        }
        reader = new ROCrateReader(uri, base);
        byPredicate = new HashMap<>();
        BufferAllocator allocator = AllocatorCore.getInstance().getChildAllocator(uri);
        manifest = reader.getManifest();
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select ?file where {
                ?s a bg:BeakGraph; so:hasPart ?file
               filter (!contains(str(?file), "dictionary"))
                filter (!contains(str(?file), "namedgraphs"))
            }
            """);
        pss.setNsPrefix("bg", BG.NS);
        pss.setNsPrefix("rdfs", RDFS.uri);
        pss.setNsPrefix("so", SchemaDO.NS);
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(), manifest);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();
            String x = qs.get("file").asResource().getURI();
            URI urix;
            try {
                urix = new URI(x);
            } catch (URISyntaxException ex) {
                throw new Error("BAD URI : "+x);
            }
            if (usemapper) {
                x = mapper.Base2Src(urix);
            } else {
                x = urix.getPath();
            }
            try {
                SeekableByteChannel xxx = reader.getSeekableByteChannel(x);
                ArrowFileReader afr = new ArrowFileReader(xxx, allocator, CommonsCompressionFactory.INSTANCE);
                VectorSchemaRoot za = afr.getVectorSchemaRoot();
                StructVector v = (StructVector) za.getVector(0);
                Map<String,String> meta = afr.getMetaData();
                String p = v.getName();
                StructVector sv = (StructVector) v.getChild("so");
                String wow = sv.getChild("o").getField().getMetadata().get(RDFS.range.getURI());
                numtriples = numtriples + Integer.parseInt(meta.get("triples"));
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAR(p, this));
                }
                PAR par = byPredicate.get(p);
                par.put(DataType.getTypeFromIRI(wow), afr);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(ROCrateReader.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(ROCrateReader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        nodeTable = new NodeTable();
        try (SeekableByteChannel d = reader.getSeekableByteChannel(uri.getPath()+"/halcyon/"+DICTIONARY)) {
            ArrowFileReader afr = new ArrowFileReader(d, allocator, CommonsCompressionFactory.INSTANCE);
            VectorSchemaRoot za = afr.getVectorSchemaRoot();
            afr.loadNextBatch();
            nodeTable.setDictionaryVectors((VarCharVector) za.getVector(0), (IntVector) za.getVector(1), (VarCharVector) za.getVector(2), (IntVector) za.getVector(3));
        }
        try (SeekableByteChannel d = reader.getSeekableByteChannel(uri.getPath()+"/halcyon/"+NAMEDGRAPHS)) {
            ArrowFileReader afr = new ArrowFileReader(d, allocator, CommonsCompressionFactory.INSTANCE);
            VectorSchemaRoot za = afr.getVectorSchemaRoot();
            afr.loadNextBatch();
            nodeTable.setNamedGraphVector((IntVector) za.getVector(0));
        }
        sw.Lapse("Manifest Loaded");
    }
    
    public URI getURI() {
        return uri;
    }
     
    public void warm(String predicate, int ng) {
        if (byPredicate.containsKey(predicate)) {
            byPredicate.get(predicate).getAllTypes(ng);
        }
    }
    
    public Iterator<Node> listGraphNodes() {
        return nodeTable.listGraphNodes();
    }
    
    public Model getManifest() {
        return manifest;
    }
    
    public ROCrateReader getROCReader() {
        return reader;
    }
    
    @Override
    public void close() {
        byPredicate.forEach((k,v)->{
            v.close();
        });
        try (nodeTable) {}
        reader.close();
        byPredicate.clear();
    }
    
    public HashMap<String,PAR> getPredicates() {
        return byPredicate;
    }
    
    public int getNumberOfTriples() {
        return numtriples;
    }

    public NodeTable getNodeTable() {
        return nodeTable;
    }

    public Iterator<BindingNodeId> Read(int ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        if (!triple.getPredicate().isVariable()) {            
            if (byPredicate.containsKey(triple.getPredicate().getURI())) {
                ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
                byPredicate.get(triple.getPredicate().getURI()).getAllTypes(ng).forEach((k,dual)->{
                    Iterator<BindingNodeId> i = new BeakIterator(bnid, k, dual, triple, filter, nodeTable, triple.getPredicate());
                    its.add(i);
                });
                return new IteratorChain(its);
            }
            return new IteratorChain(new ArrayList<>());
        }
        ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
        byPredicate.forEach((s,par)->{            
            par.getAllTypes(ng).forEach((k,dual)->{
                Iterator<BindingNodeId> i = new BeakIterator(bnid, k, dual, triple, filter, nodeTable, par.getPredicateNode());
                its.add(i);
            });
        });
        return new IteratorChain(its);
    }
    
    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BeakReader other = (BeakReader) obj;
        return Objects.equals(this.uri, other.uri);
    }
}
