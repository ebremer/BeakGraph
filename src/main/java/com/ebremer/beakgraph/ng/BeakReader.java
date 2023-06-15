package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.BeakGraph.DICTIONARY;
import static com.ebremer.beakgraph.ng.BeakGraph.NAMEDGRAPHS;
import com.ebremer.rocrate4j.ROCrateReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
    private final BufferAllocator root;
    private final Model manifest;
    private int numtriples = 0;
    private final ROCrateReader reader;
    private int currentGraph = 0;
    
    public BeakReader(URI uri) throws FileNotFoundException, IOException {
        reader = new ROCrateReader(uri);
        byPredicate = new HashMap<>();
        root = new RootAllocator();
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
            try {
                SeekableByteChannel xxx = reader.getSeekableByteChannel(x);
                ArrowFileReader afr = new ArrowFileReader(xxx, root, CommonsCompressionFactory.INSTANCE);
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
        String cha = uri.toString();
        if (cha.startsWith("file:/")) {
            if (!cha.startsWith("file://")) {
                cha = "file:///"+cha.substring("file:/".length());
            }
        }
        nodeTable = new NodeTable();
        try (SeekableByteChannel d = reader.getSeekableByteChannel(cha+"/halcyon/"+DICTIONARY)) {
            ArrowFileReader afr = new ArrowFileReader(d, root, CommonsCompressionFactory.INSTANCE);
            VectorSchemaRoot za = afr.getVectorSchemaRoot();
            afr.loadNextBatch();
            nodeTable.setDictionaryVectors((VarCharVector) za.getVector(0), (IntVector) za.getVector(1), (VarCharVector) za.getVector(2), (IntVector) za.getVector(3));
        }
        try (SeekableByteChannel d = reader.getSeekableByteChannel(cha+"/halcyon/"+NAMEDGRAPHS)) {
            ArrowFileReader afr = new ArrowFileReader(d, root, CommonsCompressionFactory.INSTANCE);
            VectorSchemaRoot za = afr.getVectorSchemaRoot();
            afr.loadNextBatch();
            nodeTable.setNamedGraphVector((IntVector) za.getVector(0));
        }
    }
    
    public BufferAllocator getBufferAllocator() {
        return root;
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
        root.close();
        reader.close();
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
        if (byPredicate.containsKey(triple.getPredicate().getURI())) {
            ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
            byPredicate.get(triple.getPredicate().getURI()).getAllTypes(ng).forEach((k,dual)->{
                Iterator<BindingNodeId> i = new BeakIterator(bnid, k, dual, triple, filter, nodeTable);
                its.add(i);
            });
            return new IteratorChain(its);
        }
        return new IteratorChain(new ArrayList<>());
    }
}

/*
    public Iterator<BindingNodeId> Read(BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        if (byPredicate.containsKey(triple.getPredicate().getURI())) {
            ArrayList<Iterator<BindingNodeId>> its = new ArrayList<>();
            byPredicate.get(triple.getPredicate().getURI()).getAllTypes().forEach((k,dual)->{
                Iterator<BindingNodeId> i = new BeakIterator(bnid, k, dual, triple, filter, nodeTable);
                its.add(i);
            });
            return new IteratorChain(its);
        }
        return new IteratorChain(new ArrayList<>());
    }
*/