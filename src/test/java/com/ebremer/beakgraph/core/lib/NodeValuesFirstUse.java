package com.ebremer.beakgraph.core.lib;

import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.util.concurrent.CyclicBarrier;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 * What {@link NodeValuesFirstUseTest} runs in a JVM of its own: threads that
 * all make their first value at the same moment, each of another kind, before
 * anything else in the JVM has touched Jena's values. Ends with exit code 0
 * when every thread has made its values; a JVM in which the threads wait for
 * each other's class initialization never ends.
 */
public final class NodeValuesFirstUse {

    private NodeValuesFirstUse() {}

    public static void main(String[] args) throws Exception {
        Node[] terms = {
            NodeFactory.createLiteralString("text"),
            NodeFactory.createLiteralDT("5", XSDDatatype.XSDint),
            NodeFactory.createLiteralDT("1.5", XSDDatatype.XSDdouble),
            NodeFactory.createLiteralDT("1.5", XSDDatatype.XSDdecimal),
            NodeFactory.createLiteralDT("1.5", XSDDatatype.XSDfloat),
            NodeFactory.createLiteralDT("POINT (1 2)", WKTDatatype.INSTANCE),
            NodeFactory.createLiteralDT("true", XSDDatatype.XSDboolean),
            NodeFactory.createURI("http://ex.org/a")};
        int threads = 16;
        CyclicBarrier start = new CyclicBarrier(threads);
        Thread[] workers = new Thread[threads];
        int[] made = new int[threads];
        for (int t = 0; t < threads; t++) {
            int first = t;
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < terms.length; i++) {
                        if (NodeValues.of(terms[(first + i) % terms.length]) != null) {
                            made[first]++;
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            workers[t].start();
        }
        for (Thread w : workers) {
            w.join();
        }
        for (int count : made) {
            if (count != terms.length) {
                System.exit(3);
            }
        }
        System.exit(0);
    }
}
