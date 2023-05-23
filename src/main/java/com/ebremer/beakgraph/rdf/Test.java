package com.ebremer.beakgraph.rdf;

import com.ebremer.rocrate4j.ROCrate;
import com.ebremer.rocrate4j.writers.ZipWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

/**
 *
 * @author erich
 */
public class Test {
    
    public void yah() throws FileNotFoundException, IOException {
        ROCrate.ROCrateBuilder builder = new ROCrate.ROCrateBuilder(new ZipWriter(new File("D:\\halcyon\\yay.zip")));
            Model m = ModelFactory.createDefaultModel();
            RDFDataMgr.read(m, new GZIPInputStream(new FileInputStream(new File("D:\\halcyon\\src\\TCGA-AA-3872-01Z-00-DX1.eb3732ee-40e3-4ff0-a42b-d6a85cfbab6a.ttl.gz"))), Lang.TURTLE);   
            //RDFDataMgr.read(m, new FileInputStream(new File("D:\\halcyon\\src\\yay.ttl")), Lang.TURTLE);
            System.out.println("SIZE : "+m.size());
            try (BeakWriter bw = new BeakWriter(builder, "halcyon")) {
                bw.Register(m);
                bw.CreateDictionary();
                bw.Add(m);
                bw.Create(builder);
            } catch (java.lang.IllegalStateException ex) {
                System.out.println(ex.toString());
            } catch (Exception ex) {
                System.out.println(ex);
            }
            builder.build();        
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        (new Test()).yah();
        
        ArrowFileReader ha;
    }
}
