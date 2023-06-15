package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 *
 * @author erich
 */
public class DefaultProcess implements AbstractProcess {

    @Override
    public void Process(BeakWriter bw, Dataset ds) {
        bw.Analyze(ds);
        Resource dg = ResourceFactory.createResource("urn:halcyon:defaultgraph");
        bw.RegisterNamedGraph(dg);
        bw.Add(dg, ds.getDefaultModel());
        int c = 0;
        Iterator<Resource> ngs = ds.listModelNames();
        while (ngs.hasNext()) {
            ngs.next();
            c++;
        }
        ngs = ds.listModelNames();
        while (ngs.hasNext()) {
            long begin = System.nanoTime();
            Resource ng = ngs.next();
            bw.getbyPredicate().forEach((k,paw)->{
                paw.sumCounts();
                paw.resetCounts();
                paw.resetVectors();
            });
            bw.RegisterNamedGraph(ng);
            bw.Add(ng,ds.getNamedModel(ng));
            c--;
            double end = System.nanoTime() - begin;
            end = end / 1000000d;
            System.out.println(ng+" "+c+"  "+end);
        }
    }
}
