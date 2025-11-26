/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class GEO {

/**
 *  GeoSPARQL 1.1 is an OGC Standard.
 *  <p>
 *	See <a href="http://www.opengis.net/ont/geosparql">GeoSPARQL 1.1 is an OGC Standard</a>.
 *  <p>
 *  <a href="http://www.opengis.net/ont/geosparql#>Base URI and namepace</a>.
 */
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "http://www.opengis.net/ont/geosparql#";

    public static final Resource Feature = m.createResource(NS+"Feature");
    public static final Resource FeatureCollection = m.createResource(NS+"FeatureCollection");
    public static final Resource Geometry = m.createResource(NS+"Geometry");
    public static final Resource GeometryCollection = m.createResource(NS+"GeometryCollection");
    public static final Resource SpatialObject = m.createResource(NS+"SpatialObject");
    public static final Resource SpatialObjectCollection = m.createResource(NS+"SpatialObjectCollection");
    public static final Resource wktLiteral = m.createResource(NS+"wktLiteral");
    public static final Property asDGGS = m.createProperty(NS+"asDGGS");
    public static final Property asGML = m.createProperty(NS+"asGML");
    public static final Property asGeoJSON = m.createProperty(NS+"asGeoJSON");
    public static final Property asKML = m.createProperty(NS+"asKML");
    public static final Property asWKT = m.createProperty(NS+"asWKT");
    public static final Property coordinateDimension = m.createProperty(NS+"coordinateDimension");
    public static final Property defaultGeometry = m.createProperty(NS+"defaultGeometry");
    public static final Property dimension = m.createProperty(NS+"dimension");
    public static final Property ehContains = m.createProperty(NS+"ehContains");
    public static final Property ehCoveredBy = m.createProperty(NS+"ehCoveredBy");
    public static final Property ehCovers = m.createProperty(NS+"ehCovers");
    public static final Property ehDisjoint = m.createProperty(NS+"ehDisjoint");
    public static final Property ehEquals = m.createProperty(NS+"ehEquals");
    public static final Property ehInside = m.createProperty(NS+"ehInside");
    public static final Property ehMeet = m.createProperty(NS+"ehMeet");
    public static final Property ehOverlap = m.createProperty(NS+"ehOverlap");
    public static final Property hasArea = m.createProperty(NS+"hasArea");
    public static final Property hasBoundingBox = m.createProperty(NS+"hasBoundingBox");
    public static final Property hasCentroid = m.createProperty(NS+"hasCentroid");
    public static final Property hasDefaultGeometry = m.createProperty(NS+"hasDefaultGeometry");
    public static final Property hasGeometry = m.createProperty(NS+"hasGeometry");
    public static final Property hasLength = m.createProperty(NS+"hasLength");
    public static final Property hasMetricArea = m.createProperty(NS+"hasMetricArea");
    public static final Property hasMetricLength = m.createProperty(NS+"hasMetricLength");
    public static final Property hasMetricPerimeterLength = m.createProperty(NS+"hasMetricPerimeterLength");
    public static final Property hasMetricSize = m.createProperty(NS+"hasMetricSize");
    public static final Property hasMetricSpatialAccuracy = m.createProperty(NS+"hasMetricSpatialAccuracy");
    public static final Property hasMetricSpatialResolution = m.createProperty(NS+"hasMetricSpatialResolution");
    public static final Property hasMetricVolume = m.createProperty(NS+"hasMetricVolume");
    public static final Property hasPerimeterLength = m.createProperty(NS+"hasPerimeterLength");
    public static final Property hasSerialization = m.createProperty(NS+"hasSerialization");
    public static final Property hasSize = m.createProperty(NS+"hasSize");
    public static final Property hasSpatialAccuracy = m.createProperty(NS+"hasSpatialAccuracy");
    public static final Property hasSpatialResolution = m.createProperty(NS+"hasSpatialResolution");
    public static final Property hasVolume = m.createProperty(NS+"hasVolume");
    public static final Property isEmpty = m.createProperty(NS+"isEmpty");
    public static final Property isSimple = m.createProperty(NS+"isSimple");
    public static final Property rcc8dc = m.createProperty(NS+"rcc8dc");
    public static final Property rcc8ec = m.createProperty(NS+"rcc8ec");
    public static final Property rcc8eq = m.createProperty(NS+"rcc8eq");
    public static final Property rcc8ntpp = m.createProperty(NS+"rcc8ntpp");
    public static final Property rcc8ntppi = m.createProperty(NS+"rcc8ntppi");
    public static final Property rcc8po = m.createProperty(NS+"rcc8po");
    public static final Property rcc8tpp = m.createProperty(NS+"rcc8tpp");
    public static final Property rcc8tppi = m.createProperty(NS+"rcc8tppi");
    public static final Property sfContains = m.createProperty(NS+"sfContains");
    public static final Property sfCrosses = m.createProperty(NS+"sfCrosses");
    public static final Property sfDisjoint = m.createProperty(NS+"sfDisjoint");
    public static final Property sfEquals = m.createProperty(NS+"sfEquals");
    public static final Property sfIntersects = m.createProperty(NS+"sfIntersects");
    public static final Property sfOverlaps = m.createProperty(NS+"sfOverlaps");
    public static final Property sfTouches = m.createProperty(NS+"sfTouches");
    public static final Property sfWithin = m.createProperty(NS+"sfWithin");
    public static final Property spatialDimension = m.createProperty(NS+"spatialDimension");
}
