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
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Monolithic Entity Dictionary.
 * @author Erich Bremer
 */
public class PositionalDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private final DictionaryWriter entitiesdict;
    private final DictionaryWriter predicatesdict;
    private final DictionaryWriter literalsdict;    
    private final long numQuads;
    private final String name;
    private final Quad[] quads;
    private final long maxEntityId;
   
    public PositionalDictionaryWriter(PositionalDictionaryWriterBuilder builder) throws FileNotFoundException, IOException {
        name = builder.getName();
        numQuads = builder.getNumberOfQuads();
        quads = builder.getQuads();
        
        Stats stats = builder.getStats();
        IO.println(stats);
        
        // 1. Build the Monolithic Entity Dictionary (G, S, O URIs + BNodes)
        entitiesdict = new MultiTypeDictionaryWriter.Builder()
            .setName("entities")
            .setNodes(builder.getEntities())
            .setStats(builder.getStats())
            .enable(Types.IRI, Types.BNODE) // Add BNODE
            .build();
            
        // 2. Build the Isolated Predicate Dictionary (P URIs)
        predicatesdict = new MultiTypeDictionaryWriter.Builder()
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
            
        // 3. Build the Isolated Literal Dictionary (O Native Literals)
        literalsdict = new MultiTypeDictionaryWriter.Builder()
            .setName("literals")
            .setNodes(builder.getLiterals())
            .setDataTypes(builder.getDataTypes())
            .setStats(builder.getStats())
            .enable(Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING)
            .build();
            
        // Cache this for fast offset math in locateObject
        maxEntityId = entitiesdict.getNumberOfNodes();
    }
   
    public Quad[] getQuads() {
        return quads;
    }
   
    public long getNumberOfQuads() {
        return numQuads;
    }
    
    // These reflect the max bounds for BGIndex Bit packing sizing.
    public long getNumberOfGraphs() {
        return maxEntityId; 
    }
   
    public long getNumberOfSubjects() {
        return maxEntityId; 
    }
   
    public long getNumberOfPredicates() {
        return predicatesdict.getNumberOfNodes();
    }
   
    public long getNumberOfObjects() {
        return maxEntityId + literalsdict.getNumberOfNodes();
    }
   
    @Override
    public long locateGraph(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Graph : "+element);
    }
   
    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Subject : "+element);
    }
   
    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Predicate : "+element);
    }
   
    @Override
    public long locateObject(Node element) {
     //   boolean isDouble = element.isLiteral() && XSDDatatype.XSDdouble.getURI().equals(element.getLiteralDatatypeURI());
//        if (isDouble) {
  //          IO.println(element);
    //        int c = 0;
      //  }
        if (element.isLiteral()) {
            long c = ((Dictionary) literalsdict).locate(element);
            if (c > 0) return c + maxEntityId; // Offset by Entity block size
            return -1;
        } else {
            long c = ((Dictionary) entitiesdict).locate(element);
            if (c > 0) return c;
            return -1;
        }
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
        if ( entitiesdict.getNumberOfNodes() > 0 ) entitiesdict.Add( dictionary );
        if ( predicatesdict.getNumberOfNodes() > 0 ) predicatesdict.Add( dictionary );
        if ( literalsdict.getNumberOfNodes() > 0 ) literalsdict.Add( dictionary );
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

    @Override
    public Dictionary getGraphs() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Dictionary getSubjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Dictionary getPredicates() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Dictionary getObjects() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
