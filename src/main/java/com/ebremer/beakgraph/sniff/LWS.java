package com.ebremer.beakgraph.sniff;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Apache Jena vocabulary constants for W3C Linked Web Storage (LWS) v1
 * Namespace: https://www.w3.org/ns/lws#
 * Specification: https://w3c.github.io/lws-protocol/
 */
public final class LWS {

    /** LWS namespace (with trailing #) */
    public static final String NS = "https://www.w3.org/ns/lws#";

    public static String getURI() {
        return NS;
    }

    private static String uri(String localName) {
        return NS + localName;
    }

    // ================================================================
    // Classes
    // ================================================================

    public static final Resource Container        = ResourceFactory.createResource(uri("Container"));
    public static final Resource ContainerPage    = ResourceFactory.createResource(uri("ContainerPage"));
    public static final Resource DataResource     = ResourceFactory.createResource(uri("DataResource"));
    public static final Resource MetadataResource = ResourceFactory.createResource(uri("MetadataResource"));
    public static final Resource Representation   = ResourceFactory.createResource(uri("Representation"));

    // ================================================================
    // Core Object Properties
    // ================================================================

    public static final Property contains      = ResourceFactory.createProperty(NS, "contains");
    public static final Property tag           = ResourceFactory.createProperty(NS, "tag");
    public static final Property partOf        = ResourceFactory.createProperty(NS, "partOf");
    public static final Property representation = ResourceFactory.createProperty(NS, "representation");

    // ================================================================
    // Paging Navigation Properties
    // ================================================================

    public static final Property first = ResourceFactory.createProperty(NS, "first");
    public static final Property last  = ResourceFactory.createProperty(NS, "last");
    public static final Property next  = ResourceFactory.createProperty(NS, "next");
    public static final Property prev  = ResourceFactory.createProperty(NS, "prev");

    // ================================================================
    // Core Datatype Properties
    // ================================================================

    public static final Property totalItems   = ResourceFactory.createProperty(NS, "totalItems");
    public static final Property mediaType    = ResourceFactory.createProperty(NS, "mediaType");
    public static final Property sizeInBytes  = ResourceFactory.createProperty(NS, "sizeInBytes");

    private LWS() {}
    
}
