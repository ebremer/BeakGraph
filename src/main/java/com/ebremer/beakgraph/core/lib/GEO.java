/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ebremer.beakgraph.core.lib;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory; // Import added

public class GEO {

/**
 * GeoSPARQL 1.1 is an OGC Standard.
 * <p>
 * See <a href="http://www.opengis.net/ont/geosparql">GeoSPARQL 1.1 is an OGC Standard</a>.
 * <p>
 * <a href="http://www.opengis.net/ont/geosparql#>Base URI and namepace</a>.
 */
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "http://www.opengis.net/ont/geosparql#";

    public static final Resource Feature = ResourceFactory.createResource(NS + "Feature");
    public static final Resource FeatureCollection = ResourceFactory.createResource(NS + "FeatureCollection");
    public static final Resource Geometry = ResourceFactory.createResource(NS + "Geometry");
    public static final Resource GeometryCollection = ResourceFactory.createResource(NS + "GeometryCollection");
    public static final Resource SpatialObject = ResourceFactory.createResource(NS + "SpatialObject");
    public static final Resource SpatialObjectCollection = ResourceFactory.createResource(NS + "SpatialObjectCollection");
    public static final Resource wktLiteral = ResourceFactory.createResource(NS + "wktLiteral");
    
    // Properties
    public static final Property asDGGS = ResourceFactory.createProperty(NS, "asDGGS");
    public static final Property asGML = ResourceFactory.createProperty(NS, "asGML");
    public static final Property asGeoJSON = ResourceFactory.createProperty(NS, "asGeoJSON");
    public static final Property asKML = ResourceFactory.createProperty(NS, "asKML");
    public static final Property asWKT = ResourceFactory.createProperty(NS, "asWKT");
    public static final Property coordinateDimension = ResourceFactory.createProperty(NS, "coordinateDimension");
    public static final Property defaultGeometry = ResourceFactory.createProperty(NS, "defaultGeometry");
    public static final Property dimension = ResourceFactory.createProperty(NS, "dimension");
    public static final Property ehContains = ResourceFactory.createProperty(NS, "ehContains");
    public static final Property ehCoveredBy = ResourceFactory.createProperty(NS, "ehCoveredBy");
    public static final Property ehCovers = ResourceFactory.createProperty(NS, "ehCovers");
    public static final Property ehDisjoint = ResourceFactory.createProperty(NS, "ehDisjoint");
    public static final Property ehEquals = ResourceFactory.createProperty(NS, "ehEquals");
    public static final Property ehInside = ResourceFactory.createProperty(NS, "ehInside");
    public static final Property ehMeet = ResourceFactory.createProperty(NS, "ehMeet");
    public static final Property ehOverlap = ResourceFactory.createProperty(NS, "ehOverlap");
    public static final Property hasArea = ResourceFactory.createProperty(NS, "hasArea");
    public static final Property hasBoundingBox = ResourceFactory.createProperty(NS, "hasBoundingBox");
    public static final Property hasCentroid = ResourceFactory.createProperty(NS, "hasCentroid");
    public static final Property hasDefaultGeometry = ResourceFactory.createProperty(NS, "hasDefaultGeometry");
    public static final Property hasGeometry = ResourceFactory.createProperty(NS, "hasGeometry");
    public static final Property hasLength = ResourceFactory.createProperty(NS, "hasLength");
    public static final Property hasMetricArea = ResourceFactory.createProperty(NS, "hasMetricArea");
    public static final Property hasMetricLength = ResourceFactory.createProperty(NS, "hasMetricLength");
    public static final Property hasMetricPerimeterLength = ResourceFactory.createProperty(NS, "hasMetricPerimeterLength");
    public static final Property hasMetricSize = ResourceFactory.createProperty(NS, "hasMetricSize");
    public static final Property hasMetricSpatialAccuracy = ResourceFactory.createProperty(NS, "hasMetricSpatialAccuracy");
    public static final Property hasMetricSpatialResolution = ResourceFactory.createProperty(NS, "hasMetricSpatialResolution");
    public static final Property hasMetricVolume = ResourceFactory.createProperty(NS, "hasMetricVolume");
    public static final Property hasPerimeterLength = ResourceFactory.createProperty(NS, "hasPerimeterLength");
    public static final Property hasSerialization = ResourceFactory.createProperty(NS, "hasSerialization");
    public static final Property hasSize = ResourceFactory.createProperty(NS, "hasSize");
    public static final Property hasSpatialAccuracy = ResourceFactory.createProperty(NS, "hasSpatialAccuracy");
    public static final Property hasSpatialResolution = ResourceFactory.createProperty(NS, "hasSpatialResolution");
    public static final Property hasVolume = ResourceFactory.createProperty(NS, "hasVolume");
    public static final Property isEmpty = ResourceFactory.createProperty(NS, "isEmpty");
    public static final Property isSimple = ResourceFactory.createProperty(NS, "isSimple");
    public static final Property rcc8dc = ResourceFactory.createProperty(NS, "rcc8dc");
    public static final Property rcc8ec = ResourceFactory.createProperty(NS, "rcc8ec");
    public static final Property rcc8eq = ResourceFactory.createProperty(NS, "rcc8eq");
    public static final Property rcc8ntpp = ResourceFactory.createProperty(NS, "rcc8ntpp");
    public static final Property rcc8ntppi = ResourceFactory.createProperty(NS, "rcc8ntppi");
    public static final Property rcc8po = ResourceFactory.createProperty(NS, "rcc8po");
    public static final Property rcc8tpp = ResourceFactory.createProperty(NS, "rcc8tpp");
    public static final Property rcc8tppi = ResourceFactory.createProperty(NS, "rcc8tppi");
    public static final Property sfContains = ResourceFactory.createProperty(NS, "sfContains");
    public static final Property sfCrosses = ResourceFactory.createProperty(NS, "sfCrosses");
    public static final Property sfDisjoint = ResourceFactory.createProperty(NS, "sfDisjoint");
    public static final Property sfEquals = ResourceFactory.createProperty(NS, "sfEquals");
    public static final Property sfIntersects = ResourceFactory.createProperty(NS, "sfIntersects");
    public static final Property sfOverlaps = ResourceFactory.createProperty(NS, "sfOverlaps");
    public static final Property sfTouches = ResourceFactory.createProperty(NS, "sfTouches");
    public static final Property sfWithin = ResourceFactory.createProperty(NS, "sfWithin");
    public static final Property spatialDimension = ResourceFactory.createProperty(NS, "spatialDimension");
}