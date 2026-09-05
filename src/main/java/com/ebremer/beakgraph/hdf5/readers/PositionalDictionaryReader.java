package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.jhdf.api.Group;
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
    /**
     * Small per-store memo for query-time derivations that depend only on this
     * dictionary's ids (the FILTER range bounds of a pattern shape, see
     * {@code RangeBounds}): resolved once, reused across the bindings of a
     * join step and the chunks of a parallel scan.
     */
    private final Cache<Object, Object> queryMemo = Caffeine.newBuilder().maximumSize(256).build();

    public Cache<Object, Object> queryMemo() {
        return queryMemo;
    }
    private final long maxEntityId;
    // RDF 1.2 triple terms occupy a CONTIGUOUS SUFFIX of the object id space
    // (they macro-rank after every literal in the literals section). Sentinel
    // MAX_VALUE/MIN_VALUE when the store holds none, so the range test below
    // is branch-free and always false.
    private final long firstTripleTermObjectId;
    private final long lastTripleTermObjectId;
    private final BitPackedUnSignedLongBuffer graphs;
    private final BitPackedUnSignedLongBuffer subjects;
    private final BitPackedUnSignedLongBuffer objects;
    // The object "dictionary" is a thin, stateless view over the entity + literal
    // dictionaries (it reads only final fields), so build it once and reuse it instead
    // of allocating a fresh wrapper on every getObjects() call in the query hot path.
    private final Dictionary objectsDict;

    public PositionalDictionaryReader(Group dictionary) {
        Group entitiesGroup = HdfProfile.group(dictionary, Params.ENTITIES);
        Group predicatesGroup = HdfProfile.group(dictionary, Params.PREDICATES);
        Group literalsGroup = HdfProfile.group(dictionary, Params.LITERALS);
        this.entities = (entitiesGroup != null) ? new MultiTypeDictionaryReader(entitiesGroup) : null;
        this.predicates = (predicatesGroup != null) ? new MultiTypeDictionaryReader(predicatesGroup) : null;
        this.literals = (literalsGroup != null) ? new MultiTypeDictionaryReader(literalsGroup) : null;
        this.maxEntityId = (entities != null) ? entities.getNumberOfNodes() : 0;
        
        this.graphs = HdfProfile.packed(dictionary, Params.GRAPHS);
        this.subjects = HdfProfile.packed(dictionary, Params.SUBJECTS);
        this.objects = HdfProfile.packed(dictionary, Params.OBJECTS);
        this.objectsDict = makeObjectsDictionary();

        // Wire the cross-dictionary triple-term resolver (CHANGELOG.md "Format v5 design notes"):
        // a term's s/p ids live in the entities/predicates sections, and its o
        // id resolves through the object space - which recurses right back
        // through this resolver for nested triple terms.
        long ttRows = (literals != null) ? literals.tripleTermRowCount() : 0;
        if (ttRows > 0) {
            literals.setTripleTermResolver((s, p, o) ->
                org.apache.jena.graph.NodeFactory.createTripleTerm(
                    entities.extract(s), predicates.extract(p), objectsDict.extract(o)));
            this.firstTripleTermObjectId = maxEntityId + literals.getNumberOfNodes() - ttRows + 1;
            this.lastTripleTermObjectId = maxEntityId + literals.getNumberOfNodes();
        } else {
            this.firstTripleTermObjectId = Long.MAX_VALUE;
            this.lastTripleTermObjectId = Long.MIN_VALUE;
        }
    }

    /** True when {@code objectId} denotes a stored RDF 1.2 triple term (the contiguous suffix of the object space). */
    /** Whether the literals section holds any cdt:List / cdt:Map literal (see {@code Params.CDT_LEXICAL_ORDER_MIN_VERSION}). */
    public boolean literalsContainCompositeDatatype() {
        return literals != null && literals.hasCompositeDatatype();
    }

    public boolean isTripleTermObjectId(long objectId) {
        return objectId >= firstTripleTermObjectId && objectId <= lastTripleTermObjectId;
    }

    /** First object-space id of the triple-term suffix; {@code Long.MAX_VALUE} when the store holds none. */
    public long firstTripleTermObjectId() {
        return firstTripleTermObjectId;
    }

    /**
     * Component ids (s, p, o) of the stored triple term with object id
     * {@code objectId}: s in the entity space, p in the predicate space, o in
     * the object space. The caller guarantees {@link #isTripleTermObjectId}.
     */
    public long[] tripleTermComponents(long objectId) {
        return literals.tripleTermComponents(objectId - maxEntityId);
    }
    
    /**
     * Stand-in for an ABSENT dictionary section (a store built from an empty
     * source has no entities/predicates groups at all - legal per the format's
     * presence-sniffing evolution). Every lookup answers "not here" instead of
     * the callers NPE-ing: before this, ANY scan-shaped query over an empty
     * store died in ScanChunks/SimpleNodeTable on a null dictionary (found by
     * the vendored SPARQL-CDTs suite's constructDataFile tests). The shared
     * null object keeps the search contract (insertion point 1, BG-312).
     */
    private static final Dictionary EMPTY = Dictionary.EMPTY;

    /**
     * True when NO dictionary section exists - a store built from an empty
     * source with no injected metadata. Note this is not "numQuads == 0": the
     * numQuads attribute counts SOURCE quads only, and a -void build of an
     * empty source has zero source quads but real stored metadata quads.
     */
    public boolean isEmpty() {
        return entities == null && predicates == null && literals == null;
    }

    @Override
    public Dictionary getGraphs() {
        return (entities != null) ? entities : EMPTY; // Graphs share the universal Entity ID space
    }

    @Override
    public Dictionary getSubjects() {
        return (entities != null) ? entities : EMPTY; // Subjects share the universal Entity ID space
    }

    @Override
    public Dictionary getPredicates() {
        return (predicates != null) ? predicates : EMPTY; // Predicates are isolated to save bit-width
    }
    
    @Override
    public Dictionary getObjects() {
        return objectsDict;
    }

    private Dictionary makeObjectsDictionary() {
        return new Dictionary() {
            @Override
            public boolean hasFloatLiterals() {
                return literals != null && literals.hasFloatLiterals();
            }

            @Override
            public boolean hasDoubleLiterals() {
                return literals != null && literals.hasDoubleLiterals();
            }

            @Override
            public long locate(Node element) {
                long result = search(element);
                return (result >= 0) ? result : -1;
            }

            @Override
            public long search(Node element) {
                // Triple terms live in the literals section too (its contiguous
                // suffix), so they share the literal routing here.
                if (element.isLiteral() || element.isTripleTerm()) {
                    // No literals section: every literal ranks after the whole
                    // entity block, so the insertion point is maxEntityId + 1 -
                    // exactly what a present-but-smaller section would answer.
                    // The former -1 (insertion point 0) made a FILTER(?o > n)
                    // over an all-IRI store scan every object row (BG-351).
                    if (literals == null) return -(maxEntityId + 1) - 1;
                    long id = literals.search(element);
                    if (id >= 1) {
                        return id + maxEntityId; // Offset by the Entity block
                    }
                    // Adjust the insertion point to account for the Entity block offset
                    long localInsertion = (-id) - 1;
                    long combinedInsertion = localInsertion + maxEntityId;
                    return -(combinedInsertion) - 1;
                } else {
                    if (entities == null) return -2; // insertion point 1 of an empty 1-based space
                    // URIs and BNodes just search the raw Entity space
                    return entities.search(element);
                }
            }

            @Override
            public Node extract(long id) {
                if (id < 1) throw new IllegalArgumentException("Cannot find Object ID: " + id);
                if (id <= maxEntityId) {
                    if (entities != null) return entities.extract(id);
                } else {
                    if (literals != null) return literals.extract(id - maxEntityId);
                }
                throw new IllegalArgumentException("Cannot find Object ID: " + id);
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
    
    /**
     * True when {@code entityId} appears in the columnar list of actual graphs.
     * Graphs share the universal entity ID space, so a bare dictionary lookup
     * cannot distinguish a graph from any other entity - this can. The list is
     * stored in ascending id order (SPECIFICATIONS.md §7.8, every engine sorts
     * it under NodeComparator), so membership is a binary search over the
     * bit-packed bytes: no boxed set per open reader (BG-252).
     */
    public boolean isGraph(long entityId) {
        if (graphs == null || entityId < 1) {
            return false;
        }
        long n = graphs.getNumEntries();
        return n > 0 && graphs.binarySearch(0, n - 1, entityId) >= 0;
    }

    @Override
    public long subjectCount() {
        return (subjects == null) ? 0 : subjects.getNumEntries();
    }

    @Override
    public long objectCount() {
        return (objects == null) ? 0 : objects.getNumEntries();
    }

    /** Raw ids of the actual graphs (the columnar list), in stored order. */
    public java.util.stream.LongStream streamGraphIds() {
        return (graphs == null) ? java.util.stream.LongStream.empty() : graphs.stream();
    }

    @Override
    public Stream<Node> streamGraphs() {
        if (graphs == null || entities == null) {
            return Stream.empty();
        }    
        return graphs.stream().mapToObj(entities::extract);
    }

    @Override
    public Stream<Node> streamSubjects() {
        // Stream only the entities that actually occur as a subject (the `subjects`
        // columnar id list), not every entity. Streaming all entities would make
        // SELECT DISTINCT ?s over-report nodes that appear only as object or graph.
        if (subjects == null || entities == null) {
            return Stream.empty();
        }
        return subjects.stream().mapToObj(entities::extract);
    }

    @Override
    public Stream<Node> streamPredicates() {
        return (predicates != null) ? predicates.streamNodes() : Stream.empty();
    }

    @Override
    public Stream<Node> streamObjects() {
        // Stream only the ids that actually occur as an object (the `objects`
        // columnar id list), not the entire entity+literal dictionary. The latter
        // would make SELECT DISTINCT ?o over-report nodes that never appear as object.
        if (objects == null) {
            return Stream.empty();
        }
        Dictionary objs = getObjects();
        return objects.stream().mapToObj(objs::extract);
    }

    @Override
    public long locateGraph(Node element) {
        return getGraphs().locate(element);
    }


    @Override
    public long locateSubject(Node element) {
        return getSubjects().locate(element);
    }


    @Override
    public long locatePredicate(Node element) {
        return getPredicates().locate(element);
    }


    @Override
    public long locateObject(Node element) {
        return getObjects().locate(element);
    }

}
