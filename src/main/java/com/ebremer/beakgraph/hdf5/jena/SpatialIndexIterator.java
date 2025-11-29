package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Var;
import org.davidmoten.hilbert.Range;

public class SpatialIndexIterator implements Iterator<BindingNodeId> {
    private final Iterator<BindingNodeId> outputIterator;

    /**
     * @param input The incoming stream of bindings
     * @param bGraph The graph instance
     * @param targetVar The specific variable to bind (e.g. ?geo). DO NOT guess this from a triple.
     * @param wkt The search polygon string
     */
    public SpatialIndexIterator(Iterator<BindingNodeId> input, BeakGraph bGraph, Var targetVar, String wkt) {
        // We no longer guess the variable. We trust the PatternMatchBG to pass the correct one.
        // Pre-fetch all Node IDs that match the Hilbert ranges.
        List<NodeId> candidateIds = fetchCandidateIds(bGraph, wkt);
        // Create the iterator pipeline.
        this.outputIterator = Iter.flatMap(input, parent -> {
            List<BindingNodeId> joined = new ArrayList<>(candidateIds.size());
            for (NodeId nodeId : candidateIds) {
                // Create a new child binding from the parent
                BindingNodeId child = new BindingNodeId(parent);
                // Bind the geometry variable to the found Node ID
                child.put(targetVar, nodeId);                
                joined.add(child);
            }
            
            return joined.iterator();
        });
    }

    @Override
    public boolean hasNext() {
        return outputIterator.hasNext();
    }

    @Override
    public BindingNodeId next() {
        return outputIterator.next();
    }
    
    private int scale = 0;

    private List<NodeId> fetchCandidateIds(BeakGraph bGraph, String wkt) {
        ArrayList<Range> ranges = HilbertPolygon.Polygon2Hilbert(wkt,scale);        
        List<NodeId> results = new ArrayList<>(300);
        NodeTable nt = bGraph.getReader().getNodeTable();
        Dataset ds = bGraph.getDataset();
        IO.println("RANGES : "+ranges.size());
        for (Range r : ranges) {
            ParameterizedSparqlString pss = getQuery(r);
            try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                ResultSet rs = qexec.execSelect();
                while (rs.hasNext()) {
                    QuerySolution qs = rs.next();
                    Node node = qs.getResource("geo").asNode();
                    NodeId nn = nt.getNodeIdForNode(node);
                    results.add(nn);
                }
            }
        }
         IO.println("RESULTS : "+results.size());
        return results;
    }

    private ParameterizedSparqlString getQuery(Range r) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select distinct ?geo
            where {                
                graph ?spatial {
                    ?geo hal:hilbertCorner ?index .
                    filter (?index >= ?a)
                    filter (?index <= ?b)
                }
            }
            """
        );
        pss.setNsPrefix("hal", "https://halcyon.is/ns/");
        pss.setIri("spatial", Params.SPATIALSTRING);
        pss.setLiteral("a", r.low());
        pss.setLiteral("b", r.high());
        //pss.setLiteral("s", scale);
        return pss;
    }
}
/*
            """
            select distinct ?geo
            where {                
                graph ?spatial {
                    ?range hal:low?s ?low .
                    ?hilbert hal:hasRange?s ?range .
                    ?geo hal:asHilbert?s ?hilbert .
                    filter (?low >= ?a)
                    filter (?low <= ?b)
                }
            }
            """
*/