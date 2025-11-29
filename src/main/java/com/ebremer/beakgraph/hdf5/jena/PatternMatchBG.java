package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.GEOF;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprList;
import static org.apache.jena.sparql.engine.main.solver.SolverLib.makeAbortable;
import org.apache.jena.sparql.expr.E_Function;

/**
 * Optimized BGP Pattern Matcher for BeakGraph.
 * Handles Spatial Index injection for geof:sfIntersect queries.
 */
public class PatternMatchBG {

    private static final String SF_INTERSECTS = GEOF.sfIntersects.getURI();

    public static QueryIterator execute(BeakGraph bGraph, BasicPattern bgp, QueryIterator input, ExprList filter, ExecutionContext execCxt) {
        List<Triple> triples = new ArrayList<>(bgp.getList());
        List<Abortable> killList = new ArrayList<>();
        Iterator<BindingNodeId> chain = Iter.map(input, SolverLibBeak.convFromBinding(bGraph));
        SpatialContext spatialCtx = getSpatialContext(filter);
        if (spatialCtx != null) {
            Triple triggerTriple = findTriggerTriple(triples, spatialCtx.geometryVar);            
            if (triggerTriple != null) {
                Var varToBind = spatialCtx.geometryVar;
                if (triggerTriple.getObject().isVariable() && triggerTriple.getObject().equals(spatialCtx.geometryVar)) {
                    if (triggerTriple.getSubject().isVariable()) {
                        varToBind = (Var) triggerTriple.getSubject();
                    }
                }
                chain = new SpatialIndexIterator(chain, bGraph, varToBind, spatialCtx.searchRegionWKT);
            }
        }
        for (Triple triple : triples) {
            chain = solve(bGraph, triple, filter, chain, execCxt);
            chain = makeAbortable(chain, killList);
        }
        Iterator<Binding> iterBinding = SolverLibBeak.convertToNodes(chain, bGraph);
        iterBinding = makeAbortable(iterBinding, killList);
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static Iterator<BindingNodeId> solve(BeakGraph bGraph, Triple triple, ExprList filter, Iterator<BindingNodeId> chain, ExecutionContext execCxt) {       
        Function<BindingNodeId, Iterator<BindingNodeId>> step = bnid -> find(bGraph, bnid, triple, filter, execCxt);
        return Iter.flatMap(chain, step);
    }
    
    private static Iterator<BindingNodeId> find(BeakGraph bGraph, BindingNodeId bnid, Triple xPattern, ExprList filter, ExecutionContext execCxt) {
        return bGraph.getReader().Read(bGraph.getNamedGraph(), bnid, xPattern, filter, bGraph.getReader().getNodeTable());
    }

    private static class SpatialContext {
        Var geometryVar;
        String searchRegionWKT;

        public SpatialContext(Var v, String wkt) {
            this.geometryVar = v;
            this.searchRegionWKT = wkt;
        }
    }

    /**
     * Analyzes the filter list to find geof:sfIntersects(?var, "POLYGON...")
     */
    private static SpatialContext getSpatialContext(ExprList filters) {
        if (filters == null) return null;

        for (Expr e : filters) {
            if (e.isFunction()) {
                ExprFunction func = e.getFunction();
                // Check if function is E_Function (Standard Jena Function)
                if (func instanceof E_Function) {
                    if (func.getFunctionIRI().equals(SF_INTERSECTS)) {
                        
                        // We expect args: [0] = ?geometryVar, [1] = ?searchRegion (or Literal)
                        if (func.getArgs().size() == 2) {
                            Expr arg0 = func.getArgs().get(0);
                            Expr arg1 = func.getArgs().get(1);

                            // Ensure first arg is a variable
                            if (arg0.isVariable()) {
                                Var targetVar = arg0.asVar();
                                String wktString = null;

                                // Resolve the second argument (The Polygon)
                                if (arg1.isConstant()) {
                                    // It's a literal directly in the filter
                                    wktString = arg1.getConstant().asNode().getLiteralLexicalForm();
                                } else if (arg1.isVariable()) {
                                    // Complex BIND handling would go here, skipping for now to ensure safety
                                }

                                if (wktString != null) {
                                    return new SpatialContext(targetVar, wktString);
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Finds the triple in the BGP that produces the geometry variable.
     * e.g. if filter is on ?wkt, find { ?s ?p ?wkt }
     */
    private static Triple findTriggerTriple(List<Triple> triples, Var targetVar) {
        for (Triple t : triples) {
            // Check Object position (most likely for geometry WKT)
            if (t.getObject().isVariable() && t.getObject().getName().equals(targetVar.getName())) {
                return t;
            }
            // Check Subject position (if filtering directly on the Feature URI)
            if (t.getSubject().isVariable() && t.getSubject().getName().equals(targetVar.getName())) {
                return t;
            }
        }
        return null;
    }
}