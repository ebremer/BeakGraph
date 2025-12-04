package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.hdf5.EmptyDictionary;
import io.jhdf.api.Group;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 * A dictionary composed of five sections: shared, subjects, predicates, objects, and graphs.
 * @author Erich Bremer
 */
public class FiveSectionDictionaryReader implements GSPODictionary {
    private final MultiTypeDictionaryReader graphs;
    private final AbstractDictionary shared;
    private final MultiTypeDictionaryReader subjects;
    private final MultiTypeDictionaryReader predicates;
    private final MultiTypeDictionaryReader objects;

    public FiveSectionDictionaryReader(Group dictionary) {
        Group graphsGroup = (Group) dictionary.getChild("graphs");
        Group sharedGroup = (Group) dictionary.getChild("shared");
        Group subjectsGroup = (Group) dictionary.getChild("subjects");
        Group predicatesGroup = (Group) dictionary.getChild("predicates");
        Group objectsGroup = (Group) dictionary.getChild("objects");
        this.graphs = new MultiTypeDictionaryReader(graphsGroup);
        this.shared = (sharedGroup != null) ? new MultiTypeDictionaryReader(sharedGroup) : new EmptyDictionary();
        this.subjects = new MultiTypeDictionaryReader(subjectsGroup);
        this.predicates = new MultiTypeDictionaryReader(predicatesGroup);
        this.objects = new MultiTypeDictionaryReader(objectsGroup);
        this.subjects.setOffset( shared.getNumberOfNodes() );
        this.objects.setOffset( shared.getNumberOfNodes() + subjects.getNumberOfNodes() );
        //predicates.streamNodes().forEach(n->IO.println(n));
    }

    public Dictionary getGraphs() {
        return graphs;
    }

    public Dictionary getSubjects() {
        return new Dictionary() {
            @Override
            public long locate(Node element) {
                long result = search(element);
                return (result >= 0) ? result : -1;
            }

            @Override
            public long search(Node element) {
                long id = shared.search(element);
                if (id > 0) return id;
                long localId = subjects.search(element);
                if (localId > 0) {
                    return localId + shared.getNumberOfNodes();
                }                
                // If strictly not found in either, we usually return the insertion point in the "Local" section
                // offset by the Shared size, assuming Shared comes "before" or is a separate bucket.
                // Returning the encoded insertion point relative to the combined ID space:
                long localInsertionPoint = (-localId) - 1;
                long combinedInsertionPoint = localInsertionPoint + shared.getNumberOfNodes();
                return -(combinedInsertionPoint) - 1;
            }

            @Override
            public Node extract(long id) {
                Node node = shared.extract(id);
                return (node != null) ? node : subjects.extract(id - shared.getNumberOfNodes());
            }

            @Override
            public Stream<Node> streamNodes() {
                return Stream.concat(shared.streamNodes(), subjects.streamNodes());
            }

            @Override
            public long getNumberOfNodes() {
                return shared.getNumberOfNodes() + subjects.getNumberOfNodes();
            }
        };
    }

    public Dictionary getPredicates() {
        return predicates;
    }

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
                    long id = objects.search(element);
                    if (id > 0) {
                        return id + shared.getNumberOfNodes();
                    }
                    long localInsertion = (-id) - 1;
                    long combinedInsertion = localInsertion + shared.getNumberOfNodes();
                    return -(combinedInsertion) - 1;
                }
                long id = shared.search(element);
                if (id > 0) return id;
                long localId = objects.search(element);
                if (localId > 0) {
                    return localId + shared.getNumberOfNodes();
                }
                long localInsertion = (-localId) - 1;
                long combinedInsertion = localInsertion + shared.getNumberOfNodes();
                return -(combinedInsertion) - 1;
            }

            @Override
            public Node extract(long id) {
                Node node = shared.extract(id);
                if (node != null) return node;
                node = objects.extract( id - shared.getNumberOfNodes());
                if (node != null) return node;
                throw new Error("Cannot find Object ID: " + id);
            }

            @Override
            public Stream<Node> streamNodes() {
                return Stream.concat(shared.streamNodes(), objects.streamNodes());
            }

            @Override
            public long getNumberOfNodes() {
                return shared.getNumberOfNodes() + objects.getNumberOfNodes();
            }
        };
    }
    
    @Override
    public Stream<Node> streamGraphs() {
        return graphs.streamNodes();
    }    

    @Override
    public long locateGraph(Node element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractGraph(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locateSubject(Node element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractSubject(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locatePredicate(Node element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractPredicate(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long locateObject(Node element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object extractObject(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
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
}