package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.store.StoreGroup;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
    // RDF-array sequences (format v7) occupy the object space's CONTIGUOUS
    // SUFFIX, and the triple terms the band just before it. The same sentinels
    // when the store holds none.
    private final long firstSequenceObjectId;
    private final long lastSequenceObjectId;
    private final BitPackedUnSignedLongBuffer graphs;
    private final BitPackedUnSignedLongBuffer subjects;
    private final BitPackedUnSignedLongBuffer objects;
    // The object "dictionary" is a thin, stateless view over the entity + literal
    // dictionaries (it reads only final fields), so build it once and reuse it instead
    // of allocating a fresh wrapper on every getObjects() call in the query hot path.
    private final Dictionary objectsDict;

    public PositionalDictionaryReader(StoreGroup dictionary) {
        StoreGroup entitiesGroup = StoreProfile.group(dictionary, Params.ENTITIES);
        StoreGroup predicatesGroup = StoreProfile.group(dictionary, Params.PREDICATES);
        StoreGroup literalsGroup = StoreProfile.group(dictionary, Params.LITERALS);
        this.entities = (entitiesGroup != null) ? new MultiTypeDictionaryReader(entitiesGroup) : null;
        this.predicates = (predicatesGroup != null) ? new MultiTypeDictionaryReader(predicatesGroup) : null;
        this.literals = (literalsGroup != null) ? new MultiTypeDictionaryReader(literalsGroup) : null;
        this.maxEntityId = (entities != null) ? entities.getNumberOfNodes() : 0;
        
        this.graphs = StoreProfile.packed(dictionary, Params.GRAPHS);
        this.subjects = StoreProfile.packed(dictionary, Params.SUBJECTS);
        this.objects = StoreProfile.packed(dictionary, Params.OBJECTS);
        this.objectsDict = makeObjectsDictionary();

        // Wire the cross-dictionary triple-term resolver (CHANGELOG.md "Format v5 design notes"):
        // a term's s/p ids live in the entities/predicates sections, and its o
        // id resolves through the object space - which recurses right back
        // through this resolver for nested triple terms.
        long ttRows = (literals != null) ? literals.tripleTermRowCount() : 0;
        long seqRows = (literals != null) ? literals.sequenceRowCount() : 0;
        long lastObjectId = maxEntityId + ((literals != null) ? literals.getNumberOfNodes() : 0);
        if (ttRows > 0) {
            literals.setTripleTermResolver((s, p, o) ->
                org.apache.jena.graph.NodeFactory.createTripleTerm(
                    entities.extract(s), predicates.extract(p), objectsDict.extract(o)));
            this.firstTripleTermObjectId = lastObjectId - seqRows - ttRows + 1;
            this.lastTripleTermObjectId = lastObjectId - seqRows;
        } else {
            this.firstTripleTermObjectId = Long.MAX_VALUE;
            this.lastTripleTermObjectId = Long.MIN_VALUE;
        }
        // Sequence elements live anywhere in the object space, nested
        // sequences included, so the literals section is handed the space.
        if (seqRows > 0) {
            literals.setSequenceSupport(new MultiTypeDictionaryReader.SequenceSupport() {
                @Override
                public Node extract(long objectId) {
                    return objectsDict.extract(objectId);
                }

                @Override
                public long locate(Node element) {
                    return objectsDict.locate(element);
                }
            });
            this.firstSequenceObjectId = lastObjectId - seqRows + 1;
            this.lastSequenceObjectId = lastObjectId;
        } else {
            this.firstSequenceObjectId = Long.MAX_VALUE;
            this.lastSequenceObjectId = Long.MIN_VALUE;
        }
    }

    /**
     * From now on, reads the sequences {@code /.BG/arrays} packs from their
     * arrays (RDFARRY.md §3.6); {@code arrays} gives the group, or null, on the
     * first such read. A store without {@code sequenceArrays} is unaffected.
     */
    public void enablePackedReads(java.util.function.Supplier<com.ebremer.beakgraph.store.StoreGroup> arrays) {
        if (literals != null && firstSequenceObjectId != Long.MAX_VALUE) {
            literals.enablePackedReads(arrays, firstSequenceObjectId);
        }
    }

    /**
     * From now on, reads every sequence from its canonical elements: a verifier
     * checks the canonical store, and checks {@code /.BG/arrays} against it.
     */
    public void disablePackedReads() {
        if (literals != null) {
            literals.disablePackedReads();
        }
    }

    /** The sequence at {@code objectId} from its canonical elements, never from {@code /.BG/arrays} ({@code -verify -deep}). */
    public Node extractCanonicalSequence(long objectId) {
        if (!isSequenceObjectId(objectId)) {
            throw new IllegalArgumentException("object " + objectId + " is no sequence");
        }
        return literals.extractCanonicalSequence(objectId - maxEntityId);
    }

    /** True when {@code objectId} denotes a stored RDF-array sequence (the contiguous suffix of the object space). */
    public boolean isSequenceObjectId(long objectId) {
        return objectId >= firstSequenceObjectId && objectId <= lastSequenceObjectId;
    }

    /** First object-space id of the sequence suffix; {@code Long.MAX_VALUE} when the store holds none. */
    public long firstSequenceObjectId() {
        return firstSequenceObjectId;
    }

    /** Last object-space id of the sequence suffix; {@code Long.MIN_VALUE} when the store holds none. */
    public long lastSequenceObjectId() {
        return lastSequenceObjectId;
    }

    /**
     * True when the store keeps the sequences of its tensor and ragged classes only in {@code /.BG/arrays}
     * ({@code -arrays-only}, format v8, RDFARRY.md step 9): they have no element ids, and their leaves may
     * have no dictionary rows.
     */
    public boolean arraysOnly() {
        return literals != null && literals.arraysOnly();
    }

    /** True when the sequence with object id {@code objectId} is held only in {@code /.BG/arrays} (format v8). */
    public boolean heldOnlyInArrays(long objectId) {
        return literals != null && literals.heldOnlyInArrays(objectId - maxEntityId);
    }

    /**
     * The elements of the sequence with object id {@code objectId} when it is held only in {@code /.BG/arrays}
     * (format v8), read from its class: leaves as their terms, inner levels as sequences of them, slots as
     * {@code undef}. Null for any other.
     */
    public java.util.List<Node> heldSequence(long objectId) {
        return (literals == null) ? null : literals.heldSequence(objectId - maxEntityId);
    }

    /**
     * Whether a leaf or inner sequence of the class holding sequence {@code objectId} may have a dictionary
     * row (format v8): when not, no leaf of it is equal to any term with an id.
     */
    public boolean heldSharesDictionary(long objectId) {
        return literals == null || literals.heldSharesDictionary(objectId - maxEntityId);
    }

    /**
     * The object ids {@code [first, last]} of the stored sequences beginning with the terms {@code prefix},
     * found by terms (format v8); null when none does. An empty prefix is the whole suffix.
     */
    public long[] sequencePrefixRangeByTerms(java.util.List<Node> prefix) {
        if (firstSequenceObjectId == Long.MAX_VALUE) {
            return null;
        }
        if (prefix.isEmpty()) {
            return new long[] {firstSequenceObjectId, lastSequenceObjectId};
        }
        long[] r = literals.sequencePrefixRangeByTerms(prefix);
        return (r == null) ? null : new long[] {r[0] + maxEntityId, r[1] + maxEntityId};
    }

    private record Run(String kind, long gi, long pi) {}

    private final java.util.concurrent.ConcurrentHashMap<Run, long[]> heldSequences =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The sequences held only in {@code /.BG/arrays} among the objects of graph {@code gi}'s predicate
     * {@code pi}, ascending (format v8): what {@code compute} works out, once for this reader's life. Null
     * when it found too many to keep. The element index has no entries for them, so every candidate scan of
     * that predicate reads them too.
     */
    public long[] heldSequencesAmong(long gi, long pi, java.util.function.Supplier<long[]> compute) {
        Run key = new Run("held", gi, pi);
        long[] s = heldSequences.get(key);
        if (s == null) {
            long[] r = compute.get();
            s = (r == null) ? MANY_SLOTS : r;
            if (heldSequences.size() < MAX_SLOT_RUNS) {
                heldSequences.putIfAbsent(key, s);
            }
        }
        return (s == MANY_SLOTS) ? null : s;
    }

    /** How many sequence elements of the store are empty slots: 0 keeps every slot-free fast path (RDFARRY.md §3.7). */
    public long numSlots() {
        return (literals == null) ? 0 : literals.numSlots();
    }

    /**
     * Whether readers opened from now on use the element index of a store that has one
     * ({@code beakgraph.seqindex.read}, default true): false answers every sequence pattern by the scan, to
     * compare the two or to step around the index.
     */
    public static boolean sequenceIndexReadsEnabled() {
        return !"false".equalsIgnoreCase(System.getProperty("beakgraph.seqindex.read"));
    }

    private final boolean sequenceIndexReads = sequenceIndexReadsEnabled();

    /**
     * True when the store has the element index of its sequences ({@code -seqindex}, RDFARRY.md step 8) and
     * this reader uses it ({@link #sequenceIndexReadsEnabled}).
     */
    public boolean hasSequenceIndex() {
        return sequenceIndexReads && storesSequenceIndex();
    }

    /** True when the store has the element index, whether or not this reader uses it ({@code -verify}). */
    public boolean storesSequenceIndex() {
        return literals != null && firstSequenceObjectId != Long.MAX_VALUE && literals.hasSequenceIndex();
    }

    /**
     * Where the stored sequences holding element {@code elementId} (an
     * object-space id, 0 for a slot) lie in the element index:
     * {@code {from, to}}, {@code to} exclusive, for {@link #sequenceIndexObjectId}
     * and {@link #sequenceIndexSeek}; null when no sequence holds it. The
     * caller guarantees {@link #hasSequenceIndex}.
     */
    public long[] sequencesHolding(long elementId) {
        return literals.sequenceIndexRange(elementId);
    }

    /** The object id of the sequence at position {@code q} of the element index. */
    public long sequenceIndexObjectId(long q) {
        return firstSequenceObjectId + literals.sequenceIndexOrdinal(q);
    }

    /**
     * The first position in {@code [from, to)} of the element index whose
     * sequence has object id {@code objectId} or a later one; {@code to} when
     * none has.
     */
    public long sequenceIndexSeek(long from, long to, long objectId) {
        long ordinal = objectId - firstSequenceObjectId;
        return (ordinal <= 0) ? from : literals.sequenceIndexSeek(from, to, ordinal);
    }

    /** {@code {distinct elements, (element, sequence) pairs}} of the element index; the caller guarantees one. */
    public long[] sequenceIndexSize() {
        return literals.sequenceIndexSize();
    }

    /** Element {@code k} of the element index, in ascending order of id (the first is 0 when there are slots). */
    public long sequenceIndexElement(long k) {
        return literals.sequenceIndexElement(k);
    }

    private record GraphPredicate(long gi, long pi) {}

    /** What {@link #slotSequencesAmong} keeps for a run that holds too many to be worth it. */
    private static final long[] MANY_SLOTS = new long[0];

    /** Past this many runs, {@link #slotSequencesAmong} works them out each time instead of keeping them. */
    private static final int MAX_SLOT_RUNS = 1 << 16;

    private final java.util.concurrent.ConcurrentHashMap<GraphPredicate, long[]> slotSequences =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The sequences with a slot among the objects of graph {@code gi}'s predicate {@code pi}, ascending
     * (RDFARRY.md step 8): what {@code compute} works out, once for this reader's life, since the store does
     * not change. Null when {@code compute} found too many to keep (it returns null then). The element
     * index's slot entries span the store, where a predicate's objects may hold few slots or none, and
     * they are candidates for every bound variable.
     */
    public long[] slotSequencesAmong(long gi, long pi, java.util.function.Supplier<long[]> compute) {
        GraphPredicate key = new GraphPredicate(gi, pi);
        long[] s = slotSequences.get(key);
        if (s == null) {
            long[] r = compute.get();
            s = (r == null) ? MANY_SLOTS : r;
            if (slotSequences.size() < MAX_SLOT_RUNS) {
                slotSequences.putIfAbsent(key, s);
            }
        }
        return (s == MANY_SLOTS) ? null : s;
    }

    /** The number of stored sequences: the length of the sequence suffix. */
    public long sequenceCount() {
        return (firstSequenceObjectId == Long.MAX_VALUE) ? 0 : lastSequenceObjectId - firstSequenceObjectId + 1;
    }

    /** How many stored sequences are held only in {@code /.BG/arrays} (format v8); 0 in any other store. */
    public long heldSequenceCount() {
        return (literals == null) ? 0 : literals.heldSequenceCount();
    }

    /**
     * The length of the stored sequence with object id {@code objectId}: from its class when it is held only
     * in {@code /.BG/arrays} (format v8), else from its elements' bounds. The caller guarantees
     * {@link #isSequenceObjectId}.
     */
    public long sequenceLength(long objectId) {
        java.util.List<Node> held = heldSequence(objectId);
        if (held != null) {
            return held.size();
        }
        long[] b = sequenceBounds(objectId);
        return b[1] - b[0];
    }

    /**
     * Where the elements of the stored sequence with object id
     * {@code objectId} begin and end (exclusive) in the element store; the
     * caller guarantees {@link #isSequenceObjectId}.
     */
    public long[] sequenceBounds(long objectId) {
        return literals.sequenceBounds(objectId - maxEntityId);
    }

    /** Each sequence's {@code /.BG/arrays} class in id order, 0 when unpacked; null without {@code -arrays}. */
    public long[] sequenceArrayClasses() {
        return (literals == null) ? null : literals.sequenceArrayClasses();
    }

    /** Number of sequence elements of the store, slots included. */
    public long sequenceElementCount() {
        return (literals == null) ? 0 : literals.sequenceElementCount();
    }

    /**
     * The element ids of the stored sequence with object id {@code objectId}:
     * object-space ids, 0 for an empty slot. The caller guarantees
     * {@link #isSequenceObjectId}.
     */
    public long[] sequenceElementIds(long objectId) {
        long[] bounds = literals.sequenceBounds(objectId - maxEntityId);
        long[] ids = new long[Math.toIntExact(bounds[1] - bounds[0])];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = literals.sequenceElementId(bounds[0] + i);
        }
        return ids;
    }

    /** Element {@code index} of the element store ({@link #sequenceBounds}): an object-space id, 0 for an empty slot. */
    public long sequenceElementAt(long index) {
        return literals.sequenceElementId(index);
    }

    /**
     * The object id of the stored sequence whose elements are exactly
     * {@code elementIds} (object-space ids, 0 for a slot), or -1 when the
     * store holds no such sequence. On ids: nothing is materialized.
     */
    public long locateSequence(long[] elementIds) {
        if (firstSequenceObjectId == Long.MAX_VALUE) {
            return -1;
        }
        long id = literals.searchSequenceIds(elementIds);
        return (id >= 1) ? id + maxEntityId : -1;
    }

    /**
     * The object ids {@code [first, last]} of the stored sequences that begin
     * with {@code prefix} (object-space ids, 0 for a slot), or null when none
     * does: a constant prefix is an id range (RDFARRY.md §3.3).
     */
    public long[] sequencePrefixRange(long[] prefix) {
        if (firstSequenceObjectId == Long.MAX_VALUE) {
            return null;
        }
        if (prefix.length == 0) {
            return new long[]{firstSequenceObjectId, lastSequenceObjectId};
        }
        long[] range = literals.sequencePrefixRange(prefix);
        return (range == null) ? null : new long[]{range[0] + maxEntityId, range[1] + maxEntityId};
    }

    /** Whether the literals section holds any cdt:List / cdt:Map literal (see {@code Params.CDT_LEXICAL_ORDER_MIN_VERSION}). */
    public boolean literalsContainCompositeDatatype() {
        return literals != null && literals.hasCompositeDatatype();
    }

    /** True when {@code objectId} denotes a stored RDF 1.2 triple term (a contiguous band of the object space). */
    public boolean isTripleTermObjectId(long objectId) {
        return objectId >= firstTripleTermObjectId && objectId <= lastTripleTermObjectId;
    }

    /** First object-space id of the triple-term band; {@code Long.MAX_VALUE} when the store holds none. */
    public long firstTripleTermObjectId() {
        return firstTripleTermObjectId;
    }

    /** Last object-space id of the triple-term band; {@code Long.MIN_VALUE} when the store holds none. */
    public long lastTripleTermObjectId() {
        return lastTripleTermObjectId;
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
                // Triple terms and sequences live in the literals section too
                // (a band and its suffix), so they share the literal routing here.
                if (element.isLiteral() || element.isTripleTerm() || element.isNodeSeq()) {
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
