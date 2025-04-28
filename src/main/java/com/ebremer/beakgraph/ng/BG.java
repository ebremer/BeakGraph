package com.ebremer.beakgraph.ng;

import com.ebremer.rocrate4j.ROCrate;
import com.ebremer.rocrate4j.writers.ZipWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class BG {
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "https://www.ebremer.com/beakgraph/ns/";
    public static final Resource BeakGraph = m.createResource(NS+"BeakGraph");
    public static final Resource PredicateVector = m.createResource(NS+"PredicateVector");
    public static final Resource Dictionary = m.createResource(NS+"Dictionary");
    public static final Resource NamedGraphs = m.createResource(NS+"NamedGraphs");
    public static final Property triples = m.createProperty(NS+"triples");
    public static final Property property = m.createProperty(NS+"property");
    public static final Property objecttype = m.createProperty(NS+"objecttype");
    public static final Property subjecttype = m.createProperty(NS+"subjecttype");
    public static final record PropertyAndDataType (String predicate, Resource dataType) {};
    
    public static class Builder {
        private Dataset ds;
        private ROCrate.ROCrateBuilder roc;
        private String base = "halcyon";
        private List<PropertyAndDataType> pairs;
        private List<SpecialProcess> specials;
        private AbstractProcess process = null;
        
        public Builder() {
            this.ds = null;
            this.roc = null;
            this.pairs = new ArrayList<>();
            this.specials = new ArrayList<>();
        }
               
        public Builder dataset(Dataset ds) {
            this.ds = ds;
            return this;
        }
        
        public Builder base(String base) {
            this.base = base;
            return this;
        }
        
        public Builder Extra(List<SpecialProcess> specials) {
            this.specials = specials;
            return this;
        }
        
        public Builder handle(List<PropertyAndDataType> pairs) {
            this.pairs = pairs;
            return this;
        }
        
        public Builder setProcess(AbstractProcess process) {
            this.process = process;
            return this;
        }
        
        public Builder rocratebuilder(ROCrate.ROCrateBuilder roc) {
            this.roc = roc;
            return this;
        }
        
        public Builder file(File file) {
            ROCrate.ROCrateBuilder builder = new ROCrate.ROCrateBuilder(new ZipWriter(file));
            this.roc = builder;
            return this;
        }
       
        public void build() {
            if (ds == null) {
                throw new Error("Dataset not set!");
            }
            if (roc == null) {
                throw new Error("ROCrate builder not set!");
            }
            if (base == null) {
                throw new Error("base cannot be null!");
            }
            if (process ==null) {
                process = new DefaultProcess();
            }
            try (BeakWriter bw = new BeakWriter(roc, base)) {
                bw.setSpecials(specials);
                bw.HandleThese(pairs);
                process.Process(bw, ds);
                try (FileOutputStream fos = new FileOutputStream(new File("/tcga/ultra.nt"))) {
                    RDFDataMgr.write(fos, ds, Lang.NQUADS);
                }
                bw.WriteDictionaryToFile();
                bw.WriteNGDictionaryToFile();
            }   catch (FileNotFoundException ex) {
                    Logger.getLogger(BG.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(BG.class.getName()).log(Level.SEVERE, null, ex);
                }
        }
    }
    
    public static Builder getBuilder() {
        return new Builder();
    }
}
