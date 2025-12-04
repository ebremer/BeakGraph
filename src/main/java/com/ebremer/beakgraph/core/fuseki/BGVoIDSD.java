package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.sniff.SD;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * A class for accumulating statistics over quads in an RDF dataset and generating
 * VoID and SPARQL Service Description (sd:) metadata.
 *
 * The class accepts a dataset URI in the constructor and now includes support for:
 * - void:vocabulary (namespaces of used classes and properties)
 * - void:uriSpace and void:uriRegexPattern (inferred from subject URIs)
 * - void:entities (number of typed instances)
 * - Copying existing dcterms: properties from the dataset into the description
 *
 * @author erich (adapted)
 */
public class BGVoIDSD {

    private final String datasetURI;
    private final Stats defaultStats = new Stats();
    private final ConcurrentHashMap<Node, Stats> namedStats = new ConcurrentHashMap<>();

    /**
     * Constructs a new BGVoIDSD instance representing the dataset identified by the given URI.
     *
     * @param datasetURI the URI of the dataset being analyzed (must not be null)
     */
    public BGVoIDSD(String datasetURI) {
        if (datasetURI == null || datasetURI.trim().isEmpty()) {
            throw new IllegalArgumentException("Dataset URI must not be null or empty");
        }
        this.datasetURI = datasetURI;
    }

    /**
     * Add a quad and update the corresponding graph statistics.
     */
    public void add(Quad quad) {
        Stats stats = getStatsForGraph(quad.getGraph());
        stats.add(quad);
    }

    private Stats getStatsForGraph(Node graphNode) {
        if (Quad.isDefaultGraph(graphNode)) {
            return defaultStats;
        }
        return namedStats.computeIfAbsent(graphNode, k -> new Stats());
    }

    /**
     * Generate and return a Model containing an sd:Dataset description identified by the
     * constructor-provided URI, with enhanced VoID metadata attached to the default graph and
     * each named graph.
     *
     * @return a Model describing the dataset using sd:, void:, and dcterms: vocabularies
     */
    public Model getModel() {
        Model m = ModelFactory.createDefaultModel();

        // Primary dataset resource
        Resource dataset = m.createResource(datasetURI)
                           .addProperty(RDF.type, SD.Dataset);

        // Default graph description
        Resource defaultGraphRes = m.createResource()
                                   .addProperty(RDF.type, SD.Graph);
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
        private long numtriples = 0;

        private final ConcurrentHashMap<Resource, Long> predicateCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Resource, Long> classCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Resource, HashSet<Resource>> classInstances = new ConcurrentHashMap<>();

        private final ConcurrentHashMap<Resource, Long> distinctSubjects = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Resource, Long> distinctObjects = new ConcurrentHashMap<>();

        private final Set<String> vocabNamespaces = ConcurrentHashMap.newKeySet();
        private final Set<String> subjectURIs = ConcurrentHashMap.newKeySet();

        public void add(Quad quad) {
            Node s = quad.getSubject();
            Node p = quad.getPredicate();
            Node o = quad.getObject();

            numtriples++;

            // Predicate usage and vocabulary
            Resource pRes = ResourceFactory.createResource(p.getURI());
            predicateCounts.merge(pRes, 1L, Long::sum);
            vocabNamespaces.add(getNamespaceBase(p.getURI()));

            // Distinct subjects
            if (s.isURI()) {
                String sURI = s.getURI();
                Resource sRes = ResourceFactory.createResource(sURI);
                distinctSubjects.merge(sRes, 1L, (old, one) -> old + one);
                subjectURIs.add(sURI);
            } else if (s.isBlank()) {
                // Blank node subjects are distinct but not for uriSpace/entities
                Resource sRes = ResourceFactory.createResource("_:" + s.getBlankNodeLabel());
                distinctSubjects.merge(sRes, 1L, (old, one) -> old + one);
            }

            // Distinct objects (resource only)
            if (o.isURI() || o.isBlank()) {
                String oURI = o.isURI() ? o.getURI() : "_:" + o.getBlankNodeLabel();
                Resource oRes = ResourceFactory.createResource(oURI);
                distinctObjects.merge(oRes, 1L, (old, one) -> old + one);
                if (o.isURI()) {
                    vocabNamespaces.add(getNamespaceBase(o.getURI()));
                }
            }

            // Classes and instances (rdf:type)
            if (p.equals(RDF.type.asNode()) && o.isURI()) {
                Resource clazz = ResourceFactory.createResource(o.getURI());
                classCounts.merge(clazz, 1L, Long::sum);
                vocabNamespaces.add(getNamespaceBase(o.getURI()));

                if (s.isURI()) {
                    classInstances.computeIfAbsent(clazz, c -> new HashSet<>())
                                 .add(ResourceFactory.createResource(s.getURI()));
                }
                // Blank node instances are ignored for classInstances (common VoID practice)
            }
        }

        private String getNamespaceBase(String uri) {
            int idx = uri.lastIndexOf('#');
            if (idx == -1) {
                idx = uri.lastIndexOf('/');
            }
            if (idx == -1) {
                return uri + "#"; // fallback
            }
            String ns = uri.substring(0, idx + 1);
            if (uri.charAt(idx) == '#') {
                return ns;
            }
            return ns; // includes trailing /
        }

        public void applyTo(Resource graphRes, Model m) {
            long entities = classInstances.values().stream().mapToLong(Set::size).sum();

            graphRes.addProperty(RDF.type, VOID.Dataset)
                    .addLiteral(VOID.triples, numtriples)
                    .addLiteral(VOID.classes, (long) classInstances.size())
                    .addLiteral(VOID.properties, (long) predicateCounts.size())
                    .addLiteral(VOID.distinctSubjects, (long) distinctSubjects.size())
                    .addLiteral(VOID.distinctObjects, (long) distinctObjects.size())
                    .addLiteral(VOID.entities, entities);

            // void:vocabulary
            vocabNamespaces.forEach(ns -> {
                graphRes.addProperty(VOID.vocabulary, ResourceFactory.createResource(ns));
            });

            // Infer void:uriSpace and void:uriRegexPattern from subject URIs
            if (!subjectURIs.isEmpty()) {
                String commonPrefix = longestCommonPrefix(subjectURIs);
                if (!commonPrefix.isEmpty() && commonPrefix.length() > 10) { // heuristic minimum length
                    graphRes.addProperty(VOID.uriSpace, commonPrefix);

                    // Simple regex: escape and add trailing .*
                    String regex = "^" + Pattern.quote(commonPrefix) + ".*$";
                    Literal regexLit = m.createTypedLiteral(regex, XSDDatatype.XSDstring);
                    graphRes.addLiteral(VOID.uriRegexPattern, regexLit);
                }
            }

            // Property partitions
            predicateCounts.forEach((prop, count) -> {
                graphRes.addProperty(VOID.propertyPartition,
                    graphRes.getModel().createResource()
                        .addProperty(VOID.property, prop)
                        .addLiteral(VOID.triples, count));
            });

            // Class partitions
            classInstances.forEach((clazz, instances) -> {
                graphRes.addProperty(VOID.classPartition,
                    graphRes.getModel().createResource()
                        .addProperty(VOID._class, clazz)
                        .addLiteral(VOID.entities, (long) instances.size()));
            });

            // Copy existing dcterms: properties describing this graph/dataset
            // (Assumes dcterms statements are in the graph being described; adjust if needed)
            graphRes.listProperties().toList().stream()
                .filter(st -> st.getPredicate().getNameSpace().equals(DCTerms.NS))
                .forEach(st -> {
                    Property pred = ResourceFactory.createProperty(st.getPredicate().getURI());
                    graphRes.addProperty(pred, st.getObject());
                });
        }

        private String longestCommonPrefix(Set<String> uris) {
            if (uris.isEmpty()) return "";
            String first = uris.iterator().next();
            String prefix = first;
            for (String uri : uris) {
                int len = Math.min(prefix.length(), uri.length());
                int i = 0;
                while (i < len && prefix.charAt(i) == uri.charAt(i)) i++;
                prefix = prefix.substring(0, i);
                if (prefix.isEmpty()) return "";
            }
            // Ensure prefix ends at a boundary (after / or #)
            int lastSlash = prefix.lastIndexOf('/');
            int lastHash = prefix.lastIndexOf('#');
            int cut = Math.max(lastSlash, lastHash);
            if (cut > 0) {
                prefix = prefix.substring(0, cut + 1);
            }
            return prefix;
        }
    }
}