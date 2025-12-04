package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import io.jhdf.api.WritableGroup;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 *
 * @author Erich Bremer
 */
public class PositionalDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private final DictionaryWriter subjectsdict;
    private final DictionaryWriter predicatesdict;
    private final DictionaryWriter objectsdict;
    private final DictionaryWriter graphsdict;
    private final long numQuads;
    private final String name;
    private final Quad[] quads;
   
    public PositionalDictionaryWriter(PositionalDictionaryWriterBuilder builder) throws FileNotFoundException, IOException {
        name = builder.getName();
        numQuads = builder.getNumberOfQuads();
        quads = builder.getQuads();
        MultiTypeDictionaryWriter.Builder shared = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder subjects = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder predicates = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder objects = new MultiTypeDictionaryWriter.Builder();
        MultiTypeDictionaryWriter.Builder graphs = new MultiTypeDictionaryWriter.Builder();
        Stats stats = builder.getStats();
        IO.println(stats);
        graphsdict = graphs
            .setName("graphs")
            .setNodes(builder.getGraphs())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
        subjectsdict = subjects
            .setName( "subjects")
            .setNodes( builder.getSubjects() )
            .setStats( builder.getStats() )
            .enable( Types.IRI )
            .build();
        predicatesdict = predicates
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
        objectsdict = objects
            .setName("objects")
            .setNodes(builder.getObjects())
            .setStats(builder.getStats())
            .enable( Types.IRI, Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING )
            .build();
    }
   
    public Quad[] getQuads() {
        return quads;
    }
   
    public long getNumberOfQuads() {
        return numQuads;
    }
    public long getNumberOfGraphs() {
        return graphsdict.getNumberOfNodes();
    }
   
    public long getNumberOfSubjects() {
        return subjectsdict.getNumberOfNodes();
    }
   
    public long getNumberOfPredicates() {
        return predicatesdict.getNumberOfNodes();
    }
   
    public long getNumberOfObjects() {
        return objectsdict.getNumberOfNodes();
    }
   
    @Override
    public long locateGraph(Node element) {
        long c = ((Dictionary) graphsdict).locate(element);
        if (c > 0) {
            return c;
        }
        throw new Error("Cannot resolve Graph : "+element);
    }
    
    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) subjectsdict).locate(element);
        if (c > 0) {
            return c;
        }        
        throw new Error("Cannot resolve Subject : "+element);
    }
   
    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) {
            return c;
        }
        throw new Error("Cannot resolve Predicate : "+element);
    }
    
    @Override
    public long locateObject(Node element) {
        long c;
        if (element.isLiteral()) {
            c = ((Dictionary) objectsdict).locate(element);
            if (c < 0) {
                return -1;
            }
            return c;
        }
        c = ((Dictionary) objectsdict).locate(element);
        if (c < 0) {
            return -1;
        }
        return c;
    }
    
    @Override
    public Object extractGraph(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Object extractSubject(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Object extractPredicate(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractObject(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public void close() {
       
    }
    
    @Override
    public long getNumberOfNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public List<Node> getNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public void Add(WritableGroup group) {
        WritableGroup dictionary = group.putGroup(name);
        if ( graphsdict.getNumberOfNodes() > 0 ) graphsdict.Add( dictionary );
        if ( subjectsdict.getNumberOfNodes() > 0 ) subjectsdict.Add( dictionary );
        if ( predicatesdict.getNumberOfNodes() > 0 ) predicatesdict.Add( dictionary );
        if ( objectsdict.getNumberOfNodes() > 0 ) objectsdict.Add( dictionary );
    }
    
    @Override
    public Stream<Node> streamSubjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Stream<Node> streamPredicates() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Stream<Node> streamObjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public Stream<Node> streamGraphs() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
