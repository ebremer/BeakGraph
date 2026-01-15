package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import org.apache.jena.sparql.core.Quad;
import java.util.Comparator;

public enum Index {
    // === G Indexes (Graph first) ===
    GSPO { // Graph, Subject, Predicate, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    GPSO { // Graph, Predicate, Subject, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    GPOS { // Graph, Predicate, Object, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    },
    GOSP { // Graph, Object, Subject, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    GSOE { // Graph, Subject, Object, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    GOPS { // Graph, Object, Predicate, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    },

    // === S Indexes (Subject first) ===
    SGPO { // Subject, Graph, Predicate, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    SGOP { // Subject, Graph, Object, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    SPGO { // Subject, Predicate, Graph, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    SPOG { // Subject, Predicate, Object, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    SOGP { // Subject, Object, Graph, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    SOPG { // Subject, Object, Predicate, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },

    // === P Indexes (Predicate first) ===
    PSGO { // Predicate, Subject, Graph, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    PSOG { // Predicate, Subject, Object, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    PGSO { // Predicate, Graph, Subject, Object
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE);
        }
    },
    PGOS { // Predicate, Graph, Object, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    },
    POSG { // Predicate, Object, Subject, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    POGS { // Predicate, Object, Graph, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    },

    // === O Indexes (Object first) ===
    OPSG { // Object, Predicate, Subject, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    OGSP { // Object, Graph, Subject, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    OGPS { // Object, Graph, Predicate, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    },
    OSGP { // Object, Subject, Graph, Predicate
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE);
        }
    },
    OSPG { // Object, Subject, Predicate, Graph
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE);
        }
    },
    OPGS { // Object, Predicate, Graph, Subject
        @Override
        public Comparator<Quad> getComparator() {
            return Comparator
                .comparing(Quad::getObject, NodeComparator.INSTANCE)
                .thenComparing(Quad::getPredicate, NodeComparator.INSTANCE)
                .thenComparing(Quad::getGraph, NodeComparator.INSTANCE)
                .thenComparing(Quad::getSubject, NodeComparator.INSTANCE);
        }
    };

    public abstract Comparator<Quad> getComparator();
}