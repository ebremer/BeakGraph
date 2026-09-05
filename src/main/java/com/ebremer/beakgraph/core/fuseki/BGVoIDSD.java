package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.sniff.SD;
import com.ebremer.beakgraph.utils.UTIL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;

/**
 * A class for accumulating statistics over quads in an RDF dataset and
 * generating VoID and SPARQL Service Description (sd:) metadata.
 *
 * <p>Memory is BOUNDED regardless of dataset size: distinct-subject,
 * distinct-object, and per-class instance counts are exact up to
 * {@value Stats#EXACT_LIMIT} distinct nodes per graph and then spill into
 * HyperLogLog sketches ({@link DistinctNodeCounter}); the
 * {@code void:uriSpace} common prefix is maintained incrementally (one
 * string) and {@code void:vocabulary} is derived from the predicate and
 * class namespaces - state bounded by the number of predicates and classes,
 * never by the number of objects (a hierarchical object IRI's parent path is
 * not a vocabulary, and one string per object was unbounded; BG-43). Small and medium stores therefore report byte-identical VoID
 * to previous versions; billion-quad disk builds report deterministic
 * estimates (~0.8% error) instead of holding much of their dictionary on
 * the heap. Fully thread-safe: parallel-ingest writers call {@link #add}
 * from many threads.
 */
public class BGVoIDSD {

    private final String datasetURI;
    private final int exactLimit;
    private final Stats defaultStats;
    private final ConcurrentHashMap<Node, Stats> namedStats = new ConcurrentHashMap<>();

    /**
     * Constructs a new BGVoIDSD instance with the bounded-memory (sketch)
     * counting behaviour.
     *
     * @param datasetURI the URI of the dataset being analyzed (must not be null)
     */
    public BGVoIDSD(String datasetURI) {
        this(datasetURI, Stats.EXACT_LIMIT);
    }

    private BGVoIDSD(String datasetURI, int exactLimit) {
        if (datasetURI == null || datasetURI.trim().isEmpty()) {
            throw new IllegalArgumentException("Dataset URI must not be null or empty");
        }
        this.datasetURI = datasetURI;
        this.exactLimit = exactLimit;
        this.defaultStats = new Stats(exactLimit);
    }

    /**
     * The accumulator for a writer's {@link com.ebremer.beakgraph.core.VoidMode},
     * or {@code null} for {@link com.ebremer.beakgraph.core.VoidMode#NONE}
     * (callers skip statistics entirely).
     */
    public static BGVoIDSD forMode(com.ebremer.beakgraph.core.VoidMode mode, String datasetURI) {
        return switch (mode) {
            case NONE -> null;
            case EXACT -> new BGVoIDSD(datasetURI, Integer.MAX_VALUE); // never spills to a sketch
            case SKETCH -> new BGVoIDSD(datasetURI, Stats.EXACT_LIMIT);
        };
    }

    /**
     * Add a quad and update the corresponding graph statistics.
     * @param quad The quad to add to the statistics
     */
    public void add(Quad quad) {
        Stats stats = getStatsForGraph(quad.getGraph());
        stats.add(quad);
    }

    private Stats getStatsForGraph(Node graphNode) {
        if (Quad.isDefaultGraph(graphNode)) {
            return defaultStats;
        }
        return namedStats.computeIfAbsent(graphNode, k -> new Stats(exactLimit));
    }

    /**
     * Generate and return a Model containing an sd:Dataset description.
     * @return A Jena Model containing the generated SD and VoID metadata
     */
    public Model getModel() {
        Model m = ModelFactory.createDefaultModel();
        // Primary dataset resource
        Resource dataset = m.createResource(datasetURI).addProperty(RDF.type, SD.Dataset);
        // Default graph description
        Resource defaultGraphRes = m.createResource().addProperty(RDF.type, SD.Graph);
        defaultStats.applyTo(defaultGraphRes, m);
        dataset.addProperty(SD.defaultGraph, defaultGraphRes);
        // Named graphs
        namedStats.forEach((node, stats) -> {
            if (!node.isURI()) {
                return; // Skip non-URI named graphs
            }
            Resource ngName = m.createResource(node.getURI());
            Resource ngDesc = m.createResource()
                    .addProperty(RDF.type, SD.NamedGraph)
                    .addProperty(SD.name, ngName);
            Resource graphRes = m.createResource()
                    .addProperty(RDF.type, SD.Graph);
            stats.applyTo(graphRes, m);
            ngDesc.addProperty(SD.graph, graphRes);
            dataset.addProperty(SD.namedGraph, ngDesc);
        });
        return m;
    }

    private static class Stats {
        /** Distinct nodes tracked exactly per counter before spilling to a sketch. */
        static final int EXACT_LIMIT = 1 << 16;

        private final int exactLimit;
        private final LongAdder numtriples = new LongAdder();
        private final ConcurrentHashMap<Node, Long> predicateCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Node, DistinctNodeCounter> classInstances = new ConcurrentHashMap<>();
        private final DistinctNodeCounter distinctSubjects;
        private final DistinctNodeCounter distinctObjects;
        // Incremental replacement for what used to be derived from the FULL
        // retained subject set: the running longest common prefix of absolute
        // subject URIs (one string; null = none seen yet, "" = no common prefix).
        private final AtomicReference<String> subjectPrefix = new AtomicReference<>(null);

        Stats(int exactLimit) {
            this.exactLimit = exactLimit;
            this.distinctSubjects = new DistinctNodeCounter(exactLimit);
            this.distinctObjects = new DistinctNodeCounter(exactLimit);
        }

        public void add(Quad quad) {
            numtriples.increment();
            Node sNode = quad.getSubject();
            Node pNode = quad.getPredicate();
            Node oNode = quad.getObject();
            predicateCounts.merge(pNode, 1L, Long::sum);
            distinctSubjects.add(sNode);
            distinctObjects.add(oNode);
            if (sNode.isURI() && !UTIL.isRelativeIRI(sNode.getURI())) {
                updateSubjectPrefix(sNode.getURI());
            }
            // Classes and instances (rdf:type)
            if (pNode.equals(RDF.type.asNode()) && oNode.isURI() && sNode.isURI()) {
                classInstances.computeIfAbsent(oNode, c -> new DistinctNodeCounter(exactLimit)).add(sNode);
            }
        }

        /** Running LCP over absolute subject URIs; duplicates are naturally idempotent. */
        private void updateSubjectPrefix(String uri) {
            while (true) {
                String cur = subjectPrefix.get();
                String next;
                if (cur == null) {
                    next = uri;
                } else {
                    int len = Math.min(cur.length(), uri.length());
                    int i = 0;
                    while (i < len && cur.charAt(i) == uri.charAt(i)) i++;
                    if (i == cur.length()) {
                        return; // uri extends the current prefix: nothing shrinks
                    }
                    next = cur.substring(0, i);
                }
                if (subjectPrefix.compareAndSet(cur, next)) {
                    return;
                }
            }
        }

        public void applyTo(Resource graphRes, Model m) {
            long entities = classInstances.values().stream().mapToLong(DistinctNodeCounter::count).sum();
            graphRes.addProperty(RDF.type, VOID.Dataset)
                    .addLiteral(VOID.triples, numtriples.sum())
                    .addLiteral(VOID.classes, (long) classInstances.size())
                    .addLiteral(VOID.properties, (long) predicateCounts.size())
                    .addLiteral(VOID.distinctSubjects, distinctSubjects.count())
                    .addLiteral(VOID.distinctObjects, distinctObjects.count())
                    .addLiteral(VOID.entities, entities);
            // void:vocabulary: the namespaces of the predicates and of the classes
            // (rdf:type objects) in use - the ontologies the data draws on.
            Set<String> vocabNamespaces = new HashSet<>();
            // Process Property Partitions & capture predicate namespaces
            predicateCounts.forEach((pNode, count) -> {
                if (pNode.isURI()) {
                    Property prop = ResourceFactory.createProperty(pNode.getURI());
                    if (!UTIL.isRelativeIRI(pNode.getURI())) {
                        vocabNamespaces.add(getNamespaceBase(pNode.getURI()));
                    }
                    graphRes.addProperty(VOID.propertyPartition,
                        graphRes.getModel().createResource()
                            .addProperty(VOID.property, prop)
                            .addLiteral(VOID.triples, count));
                }
            });
            // Process Class Partitions
            classInstances.forEach((cNode, instances) -> {
                if (cNode.isURI()) {
                    if (!UTIL.isRelativeIRI(cNode.getURI())) {
                        vocabNamespaces.add(getNamespaceBase(cNode.getURI()));
                    }
                    Resource clazz = ResourceFactory.createResource(cNode.getURI());
                    graphRes.addProperty(VOID.classPartition,
                        graphRes.getModel().createResource()
                            .addProperty(VOID._class, clazz)
                            .addLiteral(VOID.entities, instances.count()));
                }
            });
            // Write void:vocabulary
            vocabNamespaces.forEach(ns -> {
                graphRes.addProperty(VOID.vocabulary, ResourceFactory.createResource(ns));
            });
            // Infer void:uriSpace and void:uriRegexPattern from the running prefix
            String commonPrefix = trimToNamespace(subjectPrefix.get());
            if (commonPrefix != null && commonPrefix.length() > 10) {
                graphRes.addProperty(VOID.uriSpace, commonPrefix);
                String regex = "^" + Pattern.quote(commonPrefix) + ".*$";
                Literal regexLit = m.createTypedLiteral(regex, XSDDatatype.XSDstring);
                graphRes.addLiteral(VOID.uriRegexPattern, regexLit);
            }
            // Copy existing dcterms: properties
            graphRes.listProperties().toList().stream()
                .filter(st -> st.getPredicate().getNameSpace().equals(DCTerms.NS))
                .forEach(st -> {
                    Property pred = ResourceFactory.createProperty(st.getPredicate().getURI());
                    graphRes.addProperty(pred, st.getObject());
                });
        }

        /** Same trailing cut the retained-set version applied: back to the last '/' or '#'. */
        private static String trimToNamespace(String prefix) {
            if (prefix == null) {
                return null;
            }
            int lastSlash = prefix.lastIndexOf('/');
            int lastHash = prefix.lastIndexOf('#');
            int cut = Math.max(lastSlash, lastHash);
            if (cut > 0) {
                return prefix.substring(0, cut + 1);
            }
            return prefix;
        }

        private String getNamespaceBase(String uri) {
            int idx = uri.lastIndexOf('#');
            if (idx == -1) idx = uri.lastIndexOf('/');
            if (idx == -1) return uri + "#";
            // The substring already ends with the separator ('#' or '/').
            return uri.substring(0, idx + 1);
        }
    }
}
