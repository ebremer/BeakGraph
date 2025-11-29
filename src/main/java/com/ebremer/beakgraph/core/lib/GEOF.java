package com.ebremer.beakgraph.core.lib;

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

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;

public class GEOF {

/**
 * GeoSPARQL 1.1 is an OGC Standard.
 * <p>
 * See <a href="http://www.opengis.net/ont/geosparql">GeoSPARQL 1.1 is an OGC Standard</a>.
 * <p>
 * <a href="http://www.opengis.net/ont/geosparql#>Base URI and namepace</a>.
 */
    public static final String NS = "http://www.opengis.net/def/function/geosparql/";
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