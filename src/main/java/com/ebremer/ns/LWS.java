package com.ebremer.ns;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

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
    // W3C LWS Protocol draft, lws10-vocab). Terms that are NOT in that vocabulary
    // (ContainerPage, MetadataResource, Representation, contains, tag, partOf,
    // representation, first/last/next/prev, mediaType, sizeInBytes) were removed:
    // clients following the spec would not recognise them. Size, format and
    // modification time are deliberately NOT here either - the vocabulary defers
    // them to schema:size, as:mediaType and as:updated, which the servlet emits.
    public static final Resource Container        = ResourceFactory.createResource(NS + "Container");
    public static final Resource DataResource     = ResourceFactory.createResource(NS + "DataResource");
    public static final Resource Storage          = ResourceFactory.createResource(NS + "Storage");
    public static final Resource StorageRoot      = ResourceFactory.createResource(NS + "StorageRoot");

    // Properties
    public static final Property items          = ResourceFactory.createProperty(NS + "items");
    public static final Property totalItems     = ResourceFactory.createProperty(NS + "totalItems");

    private LWS() {}
}
