package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
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

/**
 * Optimized SpatialIndexIterator using lazy flatMapping and efficient pool management.
 * @author erich
 */
public class SpatialIndexIterator implements Iterator<BindingNodeId> {
    private final Iterator<BindingNodeId> outputIterator;
    private int scale = 0;

    /**
     * @param input The incoming stream of bindings
     * @param bGraph The graph instance
     * @param targetVar The specific variable to bind (e.g. ?geo).
     * @param wkt The search polygon string
     */
    public SpatialIndexIterator(Iterator<BindingNodeId> input, BeakGraph bGraph, Var targetVar, String wkt) {
        // Pre-fetch all Node IDs that match the Hilbert ranges.
        List<NodeId> candidateIds = fetchCandidateIds(bGraph, wkt);

        // LAZY PIPELINE: We map the input to the candidate IDs without 
        // pre-allocating large intermediate lists.
        this.outputIterator = Iter.flatMap(input, parent -> {
            return Iter.map(candidateIds.iterator(), nodeId -> {
                BindingNodeId child = new BindingNodeId(parent);
                child.put(targetVar, nodeId);
                return child;
            });
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

    /**
     * Fetches candidate NodeIds using parallel range queries against the BeakGraph pool.
     */
    private List<NodeId> fetchCandidateIds(BeakGraph bGraph, String wkt) {
        ArrayList<Range> ranges = HilbertPolygon2.Polygon2Hilbert(wkt, scale);
        URI uri = bGraph.getURI();
        //BeakGraphPool.getPool().printStatus();
        // Use parallel stream to query ranges, but borrow from pool once per task
        return ranges.stream()
            .parallel()
            .flatMap(r -> {
                List<NodeId> localResults = new ArrayList<>();
                BeakGraph bg = null;
                try {
                    bg = BeakGraphPool.getPool().borrowObject(uri.normalize());                    
                    NodeTable nt = bg.getReader().getNodeTable();
                    Dataset ds = bg.getDataset();                    
                    ParameterizedSparqlString pss = getQuery(r);
                    try (QueryExecution qexec = QueryExecutionFactory.create(pss.asQuery(), ds)) {
                        ResultSet rs = qexec.execSelect();
                        while (rs.hasNext()) {
                            QuerySolution qs = rs.next();
                            Node node = qs.get("geo").asNode();
                            localResults.add(nt.getNodeIdForNode(node));
                        }
                    }
                } catch (Exception ex) {
                    System.getLogger(SpatialIndexIterator.class.getName())
                          .log(System.Logger.Level.ERROR, "Error querying spatial index for range: " + r, ex);
                } finally {
                    if (bg != null) {
                        try {
                            BeakGraphPool.getPool().returnObject(uri.normalize(), bg);
                        } catch (Exception e) {
                            // Pool return failure
                        }
                    }
                }
                return localResults.stream();
            })
            .collect(Collectors.toList());
    }

    private ParameterizedSparqlString getQuery(Range r) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select distinct *
            where {                
                graph ?spatial {
                    ?geo hal:hilbertCorner?s ?index .
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
        pss.setLiteral("s", 0);
        return pss;
    }

    // Kept for reference or alternative scale logic
    private ParameterizedSparqlString getQuery2(Range r) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
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
        );
        pss.setNsPrefix("hal", "https://halcyon.is/ns/");
        pss.setIri("spatial", Params.SPATIALSTRING);
        pss.setLiteral("a", r.low());
        pss.setLiteral("b", r.high());
        pss.setLiteral("s", scale);
        return pss;
    }
}