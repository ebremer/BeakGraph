package com.ebremer.ns;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;

/**
 * Apache Jena vocabulary constants for W3C Linked Web Storage (LWS).
 * <p>
 * Namespace: https://www.w3.org/ns/lws#
 * <p>
 * Specification: <a href="https://w3c.github.io/lws-protocol/">LWS Protocol</a>.
 */
public final class LWS {

    /** LWS namespace (with trailing #). */
    public static final String NS = "https://www.w3.org/ns/lws#";

    public static String getURI() {
        return NS;
    }

    // Resources
    // Classes defined by the LWS 1.0 vocabulary (https://www.w3.org/ns/lws#,
    // W3C LWS Protocol editor's draft of 2026-10-05, lws10-vocab). Terms that are
    // NOT in that vocabulary (ContainerPage, MetadataResource, Representation,
    // contains, tag, partOf, first/last/next/prev, mediaType, sizeInBytes) are
    // deliberately absent: clients following the spec would not recognise them.
    public static final Resource Container        = ResourceFactory.createResource(NS + "Container");
    public static final Resource DataResource     = ResourceFactory.createResource(NS + "DataResource");
    public static final Resource Storage          = ResourceFactory.createResource(NS + "Storage");
    public static final Resource StorageRoot      = ResourceFactory.createResource(NS + "StorageRoot");

    // Properties
    public static final Property items          = ResourceFactory.createProperty(NS + "items");
    public static final Property totalItems     = ResourceFactory.createProperty(NS + "totalItems");
    /** The canonical URI of a storage; as a Link relation, what every response points at it with. */
    public static final Property storage        = ResourceFactory.createProperty(NS + "storage");

    // The vocabulary reuses external terms for a contained resource's format,
    // modification time and size - the "format", "modified" and "size" of a
    // container representation expand to these IRIs.
    public static final Property format         = DCTerms.format;
    public static final Property modified       = DCTerms.modified;
    public static final Property size           = ResourceFactory.createProperty("https://schema.org/size");

    private LWS() {}
}
