package com.ebremer.ns;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Apache Jena vocabulary constants for PyRadiomics Shape2D
 * Preferred prefix: pyr
 */
public final class PYR {
    public static final String NS = HAL.NS+"pyr/shape2d/";
    public static String getURI() { return NS; }
    private static String uri(String localName) { return NS + localName; }

    // ================================================================
    // Classes
    // ================================================================
    public static final Resource RadiomicsShape2D = ResourceFactory.createResource(uri("RadiomicsShape2D"));
    public static final Resource RadiomicsShape2DInstance = ResourceFactory.createResource(uri("RadiomicsShape2DInstance"));

    // ================================================================
    // Properties
    // ================================================================
    public static final Property MeshSurface = ResourceFactory.createProperty(NS, "MeshSurface");
    public static final Property PixelSurface = ResourceFactory.createProperty(NS, "PixelSurface");
    public static final Property Perimeter = ResourceFactory.createProperty(NS, "Perimeter");
    public static final Property PerimeterSurfaceRatio = ResourceFactory.createProperty(NS, "PerimeterSurfaceRatio");
    public static final Property Sphericity = ResourceFactory.createProperty(NS, "Sphericity");
    public static final Property SphericalDisproportion = ResourceFactory.createProperty(NS, "SphericalDisproportion");
    public static final Property Maximum2DDiameter = ResourceFactory.createProperty(NS, "Maximum2DDiameter");
    public static final Property MajorAxisLength = ResourceFactory.createProperty(NS, "MajorAxisLength");
    public static final Property MinorAxisLength = ResourceFactory.createProperty(NS, "MinorAxisLength");
    public static final Property Elongation = ResourceFactory.createProperty(NS, "Elongation");

    private PYR() {}
}
