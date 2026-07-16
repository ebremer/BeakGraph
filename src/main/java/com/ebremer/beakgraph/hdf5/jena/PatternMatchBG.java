package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.ns.GEOF;
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
        
        // Spatial index acceleration: seed the chain with recall-safe candidates
        // for the geometry's subject. The sfIntersects filter itself STAYS in the
        // plan (the OpFilter wrapper produced by TransformFilterPlacement) - it is
        // the verification stage that removes the candidates' false positives with
        // real JTS geometry. The old code removed it, which made the lossy index
        // pre-filter the final answer.
        SpatialContext spatialCtx = getSpatialContext(filter);
        if (spatialCtx != null) {
            Triple triggerTriple = findTriggerTriple(triples, spatialCtx.geometryVar);
            if (triggerTriple != null) {
                chain = new SpatialIndexIterator(chain, bGraph, (Var) triggerTriple.getSubject(), spatialCtx);
            }
        }

        // Execute all triple patterns (the ExprList is range-pushdown hints only;
        // full filter semantics are enforced by the surrounding OpFilter).
        // Jena 6: makeAbortable takes the execution's cancel signal directly,
        // mirroring Jena's own solvers. The FIRST pattern - where a scan appears
        // when the input is the single root binding - may be answered by a
        // parallel chunked scan; join steps stay sequential (per-binding index
        // lookups, not scans).
        boolean first = true;
        for (Triple triple : triples) {
            if (first) {
                first = false;
                chain = solveFirst(bGraph, triple, filter, chain, execCxt, killList);
            } else {
                chain = solve(bGraph, triple, filter, chain, execCxt);
            }
            chain = makeAbortable(chain, killList, execCxt.getCancelSignal());
        }

        // Convert back to Jena bindings
        Iterator<Binding> iterBinding = SolverLibBeak.convertToNodes(chain, bGraph);
        iterBinding = makeAbortable(iterBinding, killList, execCxt.getCancelSignal());
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static Iterator<BindingNodeId> solve(BeakGraph bGraph, Triple triple, ExprList filter,
                                                  Iterator<BindingNodeId> chain, ExecutionContext execCxt) {
        Function<BindingNodeId, Iterator<BindingNodeId>> step =
            bnid -> find(bGraph, bnid, triple, filter, execCxt);
        return Iter.flatMap(chain, step);
    }

    /**
     * First pattern of the BGP. When the input is exactly one binding (the plain
     * top-level root - by far the common case for scan queries) and the pattern
     * is scan-shaped, answer it with a chunked parallel scan; the scan is
     * registered in the kill-list so cancellation stops its workers, and close
     * reaches it through the Iter close cascade. Multi-binding inputs (spatial
     * seeding, joins) keep the ordinary lazy per-binding chaining.
     */
    private static Iterator<BindingNodeId> solveFirst(BeakGraph bGraph, Triple triple, ExprList filter,
                                                      Iterator<BindingNodeId> chain, ExecutionContext execCxt,
                                                      List<Abortable> killList) {
        if (!chain.hasNext()) {
            return chain;
        }
        BindingNodeId b0 = chain.next();
        if (chain.hasNext()) {
            return solve(bGraph, triple, filter, Iter.concat(List.of(b0).iterator(), chain), execCxt);
        }
        ParallelScan parallel = ScanChunks.tryParallel(bGraph, b0, triple, filter, execCxt);
        if (parallel != null) {
            killList.add(parallel);
            return parallel;
        }
        return find(bGraph, b0, triple, filter, execCxt);
    }
    
    private static Iterator<BindingNodeId> find(BeakGraph bGraph, BindingNodeId bnid, Triple xPattern, 
                                                ExprList filter, ExecutionContext execCxt) {
        return bGraph.getReader().read(bGraph.getNamedGraph(), bnid, xPattern, filter, 
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

    /**
     * The triple whose VARIABLE subject the spatial index can seed. The
     * candidates are geometry SUBJECT ids (the writer indexes the subject of
     * every quad with a wktLiteral object, so any row that survives the
     * sfIntersects verification has its subject in the candidate set - the
     * seeding stays recall-safe for any predicate). The geometry variable must
     * sit in the OBJECT position: it binds WKT literals, and seeding it - or
     * any other position - injects subject ids into a literal slot, so every
     * candidate row fails the triple pattern and the query silently returns
     * nothing (the old behavior whenever the subject was concrete). With no
     * variable subject there is nothing to seed: return null and leave the
     * work to the sfIntersects OpFilter, which always stays in the plan -
     * skipping the index costs speed, never rows.
     */
    private static Triple findTriggerTriple(List<Triple> triples, Var targetVar) {
        String targetName = targetVar.getName();

        for (Triple t : triples) {
            if (t.getObject().isVariable()
                    && t.getObject().getName().equals(targetName)
                    && t.getSubject().isVariable()) {
                return t;
            }
        }

        return null;
    }
}
