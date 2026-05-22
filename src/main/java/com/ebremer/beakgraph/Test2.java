package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.fuseki.RelativeIRIResolver;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

/**
 * Dumps the data graph of a BeakGraph .h5 file as Turtle.
 * <p>
 * The HDF5 dictionary stores document-relative IRIs ({@code <>}, {@code <sibling>})
 * in relative form; those are resolved here against the file's own URI before
 * serialization, since relative IRIs cannot be written to most RDF formats.
 */
public class Test2 {
    public static void main(String[] args) throws IOException {
        File src = new File("/beakgraph/dest/compressed/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }
        try (HDF5Reader reader = new HDF5Reader(src)) {
            BeakGraph bg = new BeakGraph(reader, reader.getURI(), null);
            Dataset ds = DatasetFactory.wrap(new BGDatasetGraph(bg));
            // Resolve document-relative IRIs against the .h5 file's own URI so
            // the output is valid RDF (relative IRIs have no representation in
            // N-Triples / N-Quads / RDF-XML).
            RelativeIRIResolver resolver = new RelativeIRIResolver(reader.getURI().toString());
            Model data = resolver.resolve(ds.getDefaultModel());
            File out = new File(src.getParentFile(), src.getName().replace(".ttl.h5", ".ttl"));
            try (FileOutputStream fos = new FileOutputStream(out)) {
                RDFDataMgr.write(fos, data, RDFFormat.TURTLE);
            }
            System.out.println("Wrote " + out);
        }
    }
}
