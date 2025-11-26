package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.NodeCmp;
import java.util.Comparator;

public enum Index {
    // === G Indexes (Graph first) ===
    GSPO { // Graph, Subject, Predicate, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    GPSO { // Graph, Predicate, Subject, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    GPOS { // Graph, Predicate, Object, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    },
    GOSP { // Graph, Object, Subject, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    GSOE { // Graph, Subject, Object, Predicate (Note: 'E' usually implies mismatch, assumed GSOP intent but kept GSOE name)
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    GOPS { // Graph, Object, Predicate, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    },

    // === S Indexes (Subject first) ===
    SGPO { // Subject, Graph, Predicate, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    SGOP { // Subject, Graph, Object, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    SPGO { // Subject, Predicate, Graph, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    SPOG { // Subject, Predicate, Object, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    SOGP { // Subject, Object, Graph, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    SOPG { // Subject, Object, Predicate, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },

    // === P Indexes (Predicate first) ===
    PSGO { // Predicate, Subject, Graph, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    PSOG { // Predicate, Subject, Object, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    PGSO { // Predicate, Graph, Subject, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms);
        }
    },
    PGOS { // Predicate, Graph, Object, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    },
    POSG { // Predicate, Object, Subject, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    POGS { // Predicate, Object, Graph, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    },

    // === O Indexes (Object first) ===
    OPSG { // Object, Predicate, Subject, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    OGSP { // Object, Graph, Subject, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    OGPS { // Object, Graph, Predicate, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    },
    OSGP { // Object, Subject, Graph, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms);
        }
    },
    OSPG { // Object, Subject, Predicate, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    OPGS { // Object, Predicate, Graph, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getPredicate, NodeCmp::compareRDFTerms)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeCmp::compareRDFTerms);
        }
    };

    public abstract Comparator<Quad> getComparator();
}