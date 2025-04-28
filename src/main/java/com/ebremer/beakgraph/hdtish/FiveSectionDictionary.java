package com.ebremer.beakgraph.hdtish;

import java.util.LinkedHashSet;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public class FiveSectionDictionary implements Dictionary {
    private LinkedHashSet<Node> graphs = new LinkedHashSet<>();
    private LinkedHashSet<Node> subjects = new LinkedHashSet<>();
    private LinkedHashSet<Node> shared = new LinkedHashSet<>();
    private LinkedHashSet<Node> objects = new LinkedHashSet<>();
    private LinkedHashSet<Node> predicates = new LinkedHashSet<>();
    private int numberOfGraphs = 0;
    private int numberOfSubjects = 0;
    private int numberOfPredicates = 0;
    private int numberOfObjects = 0;
    private int numberOfShared = 0;
    private FrontCodedBuffer fshared = new FrontCodedBuffer(10);
    private FrontCodedBuffer fgraphs = new FrontCodedBuffer(10);
    private FrontCodedBuffer fsubjects = new FrontCodedBuffer(10);
    private FrontCodedBuffer fpredicates = new FrontCodedBuffer(10);
    
    public FiveSectionDictionary() {
        
    }
    
    public void build() {
        numberOfShared = shared.size();
        numberOfGraphs = graphs.size();
        numberOfSubjects = subjects.size();
        numberOfPredicates = predicates.size();
        numberOfObjects = objects.size();
        shared.forEach(n->fshared.add(n.toString()));
        graphs.forEach(n->fgraphs.add(n.toString()));
        subjects.forEach(n->fsubjects.add(n.toString()));
        predicates.forEach(n->fpredicates.add(n.toString()));
    }

    public void AddGraph(Node node) {
        if (subjects.contains(node)) {
            subjects.remove(node);
            shared.add(node);
        } else if (objects.contains(node)) {
            objects.remove(node);
            shared.add(node);
        } else {
            graphs.add(node);
        }
    }
    
    public void AddSubject(Node node) {
        if (graphs.contains(node)) {
            graphs.remove(node);
            shared.add(node);
        } else if (objects.contains(node)) {
            objects.remove(node);
            shared.add(node);
        } else {
            subjects.add(node);
        }        
    }

    public void AddPredicate(Node node) {
        predicates.add(node);
    }

    public void AddObject(Node node) {
        if (node.isLiteral()) {
            objects.add(node);
        } else {
            if (graphs.contains(node)) {
                graphs.remove(node);
                objects.add(node);
            } else if (subjects.contains(node)) {
                subjects.remove(node);
                shared.add(node);
            } else {
                objects.add(node);
            }
        }
    }
    
    @Override
    public String toString() {
        return String.format("graphs: %d subjects: %d predicates: %d objects: %d shared: %s",
                graphs.size(),
                subjects.size(),
                predicates.size(),
                objects.size(),
                shared.size());
    }

    @Override
    public int locate(Node element) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Node extract(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
