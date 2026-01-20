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
 * Handles Spatial Index injection for geof:sfIntersects queries.
 */
public class PatternMatchBG {

    private static final String SF_INTERSECTS = GEOF.sfIntersects.getURI();

    public static QueryIterator execute(BeakGraph bGraph, BasicPattern bgp, QueryIterator input, ExprList filter, ExecutionContext execCxt) {
        List<Triple> triples = new ArrayList<>(bgp.getList());
        List<Abortable> killList = new ArrayList<>();
        Iterator<BindingNodeId> chain = Iter.map(input, SolverLibBeak.convFromBinding(bGraph));
        
        // Check for spatial filter optimization opportunity
        SpatialContext spatialCtx = getSpatialContext(filter);
        ExprList modifiedFilter = filter;
        Triple triggerTriple = null;
        
        if (spatialCtx != null) {
           // IO.println("SPATIAL OPTIMIZATION: var=" + spatialCtx.geometryVar + 
             //         ", region=" + spatialCtx.searchRegionWKT + ", scale=" + spatialCtx.scale);
            
            triggerTriple = findTriggerTriple(triples, spatialCtx.geometryVar);
            
            if (triggerTriple != null) {
                //IO.println("Trigger triple: " + triggerTriple);
                
                Var varToBind = spatialCtx.geometryVar;
                if (triggerTriple.getObject().isVariable() && 
                    triggerTriple.getObject().equals(spatialCtx.geometryVar)) {
                    if (triggerTriple.getSubject().isVariable()) {
                        varToBind = (Var) triggerTriple.getSubject();
                    }
                }
                
                //IO.println("Injecting SpatialIndexIterator for: " + varToBind);
                chain = new SpatialIndexIterator(chain, bGraph, varToBind, spatialCtx);
                modifiedFilter = removeSpatialFilter(filter);
            }
        }
        
        // Execute all triple patterns
        for (Triple triple : triples) {
            ExprList filterToUse = (triggerTriple != null && triple.equals(triggerTriple)) ? null : modifiedFilter;
            chain = solve(bGraph, triple, filterToUse, chain, execCxt);
            chain = makeAbortable(chain, killList);
        }
        
        // Convert back to Jena bindings
        Iterator<Binding> iterBinding = SolverLibBeak.convertToNodes(chain, bGraph);
        iterBinding = makeAbortable(iterBinding, killList);
        
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static Iterator<BindingNodeId> solve(BeakGraph bGraph, Triple triple, ExprList filter, 
                                                  Iterator<BindingNodeId> chain, ExecutionContext execCxt) {
        Function<BindingNodeId, Iterator<BindingNodeId>> step = 
            bnid -> find(bGraph, bnid, triple, filter, execCxt);
        return Iter.flatMap(chain, step);
    }
    
    private static Iterator<BindingNodeId> find(BeakGraph bGraph, BindingNodeId bnid, Triple xPattern, 
                                                ExprList filter, ExecutionContext execCxt) {
        return bGraph.getReader().Read(bGraph.getNamedGraph(), bnid, xPattern, filter, 
                                       bGraph.getReader().getNodeTable());
    }

    public static class SpatialContext {
        Var geometryVar;
        String searchRegionWKT;
        int scale;

        public SpatialContext(Var v, String wkt) {
            this(v, wkt, 0);
        }
        
        public SpatialContext(Var v, String wkt, int scale) {
            this.geometryVar = v;
            this.searchRegionWKT = wkt;
            this.scale = scale;
        }
    }

    private static SpatialContext getSpatialContext(ExprList filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        
        for (Expr e : filters) {
            if (!e.isFunction()) {
                continue;
            }
            
            ExprFunction func = e.getFunction();
            
            if (!(func instanceof E_Function)) {
                continue;
            }
            
            if (!func.getFunctionIRI().equals(SF_INTERSECTS)) {
                continue;
            }
            
            int argCount = func.getArgs().size();
            
            if (argCount == 2) {
                Expr arg0 = func.getArgs().get(0);
                Expr arg1 = func.getArgs().get(1);
                
                if (arg0.isVariable()) {
                    Var targetVar = arg0.asVar();
                    String wktString = extractWKTString(arg1);
                    
                    if (wktString != null) {
                        return new SpatialContext(targetVar, wktString);
                    }
                }
            } else if (argCount == 3) {
                Expr arg0 = func.getArgs().get(0);
                Expr arg1 = func.getArgs().get(1);
                Expr arg2 = func.getArgs().get(2);
                
                if (arg0.isVariable()) {
                    Var targetVar = arg0.asVar();
                    String wktString = extractWKTString(arg1);
                    Integer scale = extractScale(arg2);
                    
                    if (wktString != null && scale != null) {
                        return new SpatialContext(targetVar, wktString, scale);
                    }
                }
            }
        }
        
        return null;
    }

    private static String extractWKTString(Expr expr) {
        if (expr.isConstant()) {
            return expr.getConstant().asNode().getLiteralLexicalForm();
        }
        return null;
    }

    private static Integer extractScale(Expr expr) {
        if (expr.isConstant()) {
            try {
                String lexicalForm = expr.getConstant().asNode().getLiteralLexicalForm();
                return Integer.valueOf(lexicalForm);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private static ExprList removeSpatialFilter(ExprList filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        
        ExprList newFilters = new ExprList();
        
        for (Expr e : filters) {
            if (e.isFunction()) {
                ExprFunction func = e.getFunction();
                if (func instanceof E_Function) {
                    if (func.getFunctionIRI().equals(SF_INTERSECTS)) {
                        continue;
                    }
                }
            }
            newFilters.add(e);
        }
        
        return newFilters.isEmpty() ? null : newFilters;
    }

    private static Triple findTriggerTriple(List<Triple> triples, Var targetVar) {
        String targetName = targetVar.getName();
        
        for (Triple t : triples) {
            if (t.getObject().isVariable() && 
                t.getObject().getName().equals(targetName)) {
                return t;
            }
            
            if (t.getSubject().isVariable() && 
                t.getSubject().getName().equals(targetName)) {
                return t;
            }
            
            if (t.getPredicate().isVariable() && 
                t.getPredicate().getName().equals(targetName)) {
                return t;
            }
        }
        
        return null;
    }
}