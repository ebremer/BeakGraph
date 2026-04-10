package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 * A dictionary composed of three monolithic sections: entities (G, S, O URIs + BNodes), 
 * predicates (P URIs), and literals (O native values).
 * @author Erich Bremer
 */
public class PositionalDictionaryReader implements GSPODictionary {
    
    private final MultiTypeDictionaryReader entities;
    private final MultiTypeDictionaryReader predicates;
    private final MultiTypeDictionaryReader literals;
    private final long maxEntityId;
    
    private final BitPackedUnSignedLongBuffer graphs;
    private final BitPackedUnSignedLongBuffer subjects;
    private final BitPackedUnSignedLongBuffer objects;

    public PositionalDictionaryReader(Group dictionary) {
        Group entitiesGroup = (Group) dictionary.getChild("entities");
        Group predicatesGroup = (Group) dictionary.getChild("predicates");
        Group literalsGroup = (Group) dictionary.getChild("literals");        
        this.entities = (entitiesGroup != null) ? new MultiTypeDictionaryReader(entitiesGroup) : null;
        this.predicates = (predicatesGroup != null) ? new MultiTypeDictionaryReader(predicatesGroup) : null;
        this.literals = (literalsGroup != null) ? new MultiTypeDictionaryReader(literalsGroup) : null;
        this.maxEntityId = (entities != null) ? entities.getNumberOfNodes() : 0;
        
        this.graphs = getDataSet(dictionary, "graphs").map(ds ->
            new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);
        this.subjects = getDataSet(dictionary, "subjects").map(ds ->
            new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);
        this.objects = getDataSet(dictionary, "objects").map(ds ->
            new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);
        
        graphs.stream().forEach(i->IO.println(entities.extract(i)));
    }
    
    private Optional<ContiguousDataset> getDataSet(Group g, String name) {
        return (g.getChild(name) != null) ? Optional.of((ContiguousDataset) g.getChild(name)) : Optional.empty();
    }

    @Override
    public Dictionary getGraphs() {
        return entities; // Graphs share the universal Entity ID space
    }

    @Override
    public Dictionary getSubjects() {
        return entities; // Subjects share the universal Entity ID space
    }

    @Override
    public Dictionary getPredicates() {
        return predicates; // Predicates are isolated to save bit-width
    }
    
    @Override
    public Dictionary getObjects() {
        return new Dictionary() {
            @Override
            public long locate(Node element) {
                long result = search(element);
                return (result >= 0) ? result : -1;
            }

            @Override
            public long search(Node element) {
                if (element.isLiteral()) {
                    if (literals == null) return -1;
                    long id = literals.search(element);
                    if (id > 0) {
                        return id + maxEntityId; // Offset by the Entity block
                    }
                    // Adjust the insertion point to account for the Entity block offset
                    long localInsertion = (-id) - 1;
                    long combinedInsertion = localInsertion + maxEntityId;
                    return -(combinedInsertion) - 1;
                } else {
                    if (entities == null) return -1;
                    // URIs and BNodes just search the raw Entity space
                    return entities.search(element);
                }
            }

            @Override
            public Node extract(long id) {
                if (id <= maxEntityId) {
                    if (entities != null) return entities.extract(id);
                } else {
                    if (literals != null) return literals.extract(id - maxEntityId);
                }
                throw new Error("Cannot find Object ID: " + id);
            }

            @Override
            public Stream<Node> streamNodes() {
                Stream<Node> entityStream = (entities != null) ? entities.streamNodes() : Stream.empty();
                Stream<Node> literalStream = (literals != null) ? literals.streamNodes() : Stream.empty();
                return Stream.concat(entityStream, literalStream);
            }

            @Override
            public long getNumberOfNodes() {
                long eCount = (entities != null) ? entities.getNumberOfNodes() : 0;
                long lCount = (literals != null) ? literals.getNumberOfNodes() : 0;
                return eCount + lCount;
            }
        };
    }
    
    @Override
    public Stream<Node> streamGraphs() {
        return (entities != null) ? entities.streamNodes() : Stream.empty();
    }    

    @Override
    public Stream<Node> streamSubjects() {
        return (entities != null) ? entities.streamNodes() : Stream.empty();
    }

    @Override
    public Stream<Node> streamPredicates() {
        return (predicates != null) ? predicates.streamNodes() : Stream.empty();
    }

    @Override
    public Stream<Node> streamObjects() {
        return getObjects().streamNodes();
    }

    // Direct extraction and location mapping for convenience
    @Override
    public long locateGraph(Node element) {
        return getGraphs().locate(element);
    }

    @Override
    public Object extractGraph(long id) {
        return getGraphs().extract(id);
    }

    @Override
    public long locateSubject(Node element) {
        return getSubjects().locate(element);
    }

    @Override
    public Object extractSubject(long id) {
        return getSubjects().extract(id);
    }

    @Override
    public long locatePredicate(Node element) {
        return getPredicates().locate(element);
    }

    @Override
    public Object extractPredicate(long id) {
        return getPredicates().extract(id);
    }

    @Override
    public long locateObject(Node element) {
        return getObjects().locate(element);
    }

    @Override
    public Object extractObject(long id) {
        return getObjects().extract(id);
    }
}
