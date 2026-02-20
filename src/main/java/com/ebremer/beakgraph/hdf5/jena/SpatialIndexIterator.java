package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Var;
import org.davidmoten.hilbert.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimized SpatialIndexIterator using lazy flatMapping and efficient pool management.
 * @author erich
 */
public class SpatialIndexIterator implements Iterator<BindingNodeId> {
    private static final Logger logger = LoggerFactory.getLogger(SpatialIndexIterator.class);
    private final Iterator<BindingNodeId> outputIterator;
    private int scale = 0;

    public SpatialIndexIterator(Iterator<BindingNodeId> input, BeakGraph bGraph, Var targetVar, PatternMatchBG.SpatialContext context) {
        logger.trace("SpatialIndexIterator: {} at scale {}", context.searchRegionWKT, context.scale);
        List<Range> ranges = HilbertPolygon.Polygon2Hilbert(context.searchRegionWKT, 0);
        logger.trace("# of ranges : {}", ranges.size());        
        NodeTable currentTable = bGraph.getReader().getNodeTable();        
        this.outputIterator = Iter.flatMap(input, parent -> {
            Iterator<Node> lazyCandidateNodes = new LazyCandidateIterator(ranges, bGraph, context.scale);
            
            return Iter.map(lazyCandidateNodes, node -> {
                NodeId nodeId = currentTable.getNodeIdForNode(node);
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

    private static ParameterizedSparqlString buildRangeQuery(Range range, int scale) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            SELECT DISTINCT ?geo
            WHERE {                
                GRAPH ?spatial {
                    ?geo hal:hilbertCorner?scale ?index .
                    FILTER (?index >= ?low && ?index <= ?high)
                }
            }
            """
        );
        pss.setNsPrefix("hal", "https://halcyon.is/ns/");
        pss.setIri("spatial", Params.SPATIALSTRING);
        pss.setLiteral("scale", scale);
        pss.setLiteral("low", range.low());
        pss.setLiteral("high", range.high());
        return pss;
    }

    private static class LazyCandidateIterator implements Iterator<Node>, AutoCloseable {
        private final Iterator<Range> rangeIterator;
        private final BeakGraph bGraph;
        private final int scale;
        
        private QueryExecution currentQexec = null;
        private ResultSet currentResultSet = null;
        private Node nextNode = null;

        public LazyCandidateIterator(Iterable<Range> ranges, BeakGraph bGraph, int scale) {
            this.rangeIterator = ranges.iterator();
            this.bGraph = bGraph;
            this.scale = scale;
            advance();
        }

        private void advance() {
            nextNode = null;            
            while (true) {
                if (currentResultSet != null && currentResultSet.hasNext()) {
                    nextNode = currentResultSet.next().get("geo").asNode();
                    return;
                }
                if (currentQexec != null) {
                    currentQexec.close();
                    currentQexec = null;
                    currentResultSet = null;
                }
                if (!rangeIterator.hasNext()) {
                    return; 
                }
                Range range = rangeIterator.next();
                Dataset ds = bGraph.getDataset();
                ParameterizedSparqlString pss = buildRangeQuery(range, scale);
                currentQexec = QueryExecutionFactory.create(pss.asQuery(), ds);
                currentResultSet = currentQexec.execSelect();
            }
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        @Override
        public Node next() {
            if (nextNode == null) {
                throw new NoSuchElementException();
            }
            Node current = nextNode;
            advance();
            return current;
        }

        @Override
        public void close() {
            if (currentQexec != null) {
                currentQexec.close();
                currentQexec = null;
                currentResultSet = null;
            }
        }
    }
}