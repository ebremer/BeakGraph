package com.ebremer.beakgraph.core.debug;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.AsyncParser;

/**
 * Checks the raw source file for semantic validity.
 */
public class SourceInspector {
    public static void main(String[] args) {
        File file = new File("/data/sorted.nq.gz");
        System.out.println("Inspecting: " + file.getAbsolutePath());
        
        try {
            InputStream is = new FileInputStream(file);
            if (file.getName().endsWith(".gz")) {
                is = new GZIPInputStream(is);
            }
            
            System.out.println("--- First 20 Quads from Source ---");
            AsyncParser.of(is, Lang.NQUADS, null)
                .streamQuads()
                .limit(20)
                .forEach(q -> {
                    System.out.println(q);
                    if (q.getPredicate().getURI().endsWith("type") && q.getObject().isBlank()) {
                        System.out.println("  ^^^ WARNING: rdf:type pointing to Blank Node! Data corruption likely.");
                    }
                    if (q.getPredicate().getURI().endsWith("type") && q.getObject().isLiteral()) {
                        System.out.println("  ^^^ WARNING: rdf:type pointing to Literal! Data corruption confirmed.");
                    }
                });
                
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}