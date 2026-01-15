package com.ebremer.beakgraph.sniff;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Apache Jena vocabulary constants for SPARQL 1.1 Service Description (sd)
 * Namespace: http://www.w3.org/ns/sparql-service-description#
 * Specification: https://www.w3.org/TR/sparql11-service-description/
 */
public final class SD {
    /** sd namespace (with trailing #) */
    public static final String NS = "http://www.w3.org/ns/sparql-service-description#";

    public static String getURI() {
        return NS;
    }

    private static String uri(String localName) {
        return NS + localName;
    }

    // ================================================================
    // Classes
    // ================================================================

    public static final Resource Service = ResourceFactory.createResource(uri("Service"));
    public static final Resource Feature = ResourceFactory.createResource(uri("Feature"));
    public static final Resource Language = ResourceFactory.createResource(uri("Language"));
    public static final Resource Function = ResourceFactory.createResource(uri("Function"));
    public static final Resource Aggregate = ResourceFactory.createResource(uri("Aggregate"));
    public static final Resource EntailmentRegime = ResourceFactory.createResource(uri("EntailmentRegime"));
    public static final Resource EntailmentProfile = ResourceFactory.createResource(uri("EntailmentProfile"));
    public static final Resource GraphCollection = ResourceFactory.createResource(uri("GraphCollection"));
    public static final Resource Dataset = ResourceFactory.createResource(uri("Dataset"));
    public static final Resource Graph = ResourceFactory.createResource(uri("Graph"));
    public static final Resource NamedGraph = ResourceFactory.createResource(uri("NamedGraph"));

    // ================================================================
    // Properties
    // ================================================================

    public static final Property endpoint = ResourceFactory.createProperty(uri("endpoint"));
    public static final Property feature = ResourceFactory.createProperty(uri("feature"));
    public static final Property defaultEntailmentRegime = ResourceFactory.createProperty(uri("defaultEntailmentRegime"));
    public static final Property entailmentRegime = ResourceFactory.createProperty(uri("entailmentRegime"));
    public static final Property defaultSupportedEntailmentProfile = ResourceFactory.createProperty(uri("defaultSupportedEntailmentProfile"));
    public static final Property supportedEntailmentProfile = ResourceFactory.createProperty(uri("supportedEntailmentProfile"));
    public static final Property extensionFunction = ResourceFactory.createProperty(uri("extensionFunction"));
    public static final Property extensionAggregate = ResourceFactory.createProperty(uri("extensionAggregate"));
    public static final Property languageExtension = ResourceFactory.createProperty(uri("languageExtension"));
    public static final Property supportedLanguage = ResourceFactory.createProperty(uri("supportedLanguage"));
    public static final Property propertyFeature = ResourceFactory.createProperty(uri("propertyFeature"));
    public static final Property defaultDataset = ResourceFactory.createProperty(uri("defaultDataset"));
    public static final Property availableGraphs = ResourceFactory.createProperty(uri("availableGraphs"));
    public static final Property resultFormat = ResourceFactory.createProperty(uri("resultFormat"));
    public static final Property inputFormat = ResourceFactory.createProperty(uri("inputFormat"));
    public static final Property defaultGraph = ResourceFactory.createProperty(uri("defaultGraph"));
    public static final Property namedGraph = ResourceFactory.createProperty(uri("namedGraph"));
    public static final Property name = ResourceFactory.createProperty(uri("name"));
    public static final Property graph = ResourceFactory.createProperty(uri("graph"));

    private SD() {}
}