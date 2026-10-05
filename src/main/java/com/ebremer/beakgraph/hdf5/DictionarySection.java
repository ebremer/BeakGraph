package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.lib.DataType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * The three dictionary sections (SPECIFICATIONS.md §7.2) and the term kinds
 * each one routes. This is what every writer engine configures a section
 * writer with; it replaces the former {@code Types} enum, a second copy of a
 * subset of {@link DataType} with different ordinals whose enable lists were
 * hand-copied per engine and honoured inconsistently (BG-90). The on-disk
 * {@code datatypes} column stores {@link DataType} ordinals only.
 */
public enum DictionarySection {
    /** Graph names, subjects and object entities: IRIs (relative ones included) and blank nodes. */
    ENTITIES(Params.ENTITIES, EnumSet.of(DataType.IRI, DataType.RELATIVE_IRI, DataType.BNODE)),
    /** Predicates: IRIs only, in their own id space. */
    PREDICATES(Params.PREDICATES, EnumSet.of(DataType.IRI, DataType.RELATIVE_IRI)),
    /** Object literals of every stored kind, plus RDF 1.2 triple terms (the section's suffix). */
    LITERALS(Params.LITERALS, EnumSet.of(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
            DataType.STRING, DataType.TRIPLE_TERM));

    private final String groupName;
    private final Set<DataType> routes;

    DictionarySection(String groupName, EnumSet<DataType> routes) {
        this.groupName = groupName;
        this.routes = Collections.unmodifiableSet(routes);
    }

    /** The HDF5 group name under {@code .BG/dictionary}. */
    public String groupName() {
        return groupName;
    }

    /** The term kinds a writer for this section stores. */
    public Set<DataType> routes() {
        return routes;
    }
}
