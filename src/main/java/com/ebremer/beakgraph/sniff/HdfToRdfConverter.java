package com.ebremer.beakgraph.sniff;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import java.io.File;
import org.apache.jena.vocabulary.XSD;

public class HdfToRdfConverter {

    private static final String FILE_NS = "https://bmi.stonybrook.edu/files/til2025.h5";

    public static void main(String[] args) {
        File hdf5File = new File("/beakgraph/dXX.h5");
        Model m = ModelFactory.createDefaultModel();
        m.setNsPrefix("lws", LWS.NS);
        m.setNsPrefix("xsd", XSD.NS);
        try (HdfFile hdfFile = new HdfFile(hdf5File)) {
            Resource rootRdfNode = m.createResource(FILE_NS + hdfFile.getPath());
            rootRdfNode.addProperty(RDF.type, LWS.Container);
            System.out.println("Scanning HDF5 file...");
            processNode(hdfFile, rootRdfNode, m, LWS.Container, LWS.DataResource, LWS.contains);
            System.out.println("\n--- Generated RDF (Turtle) ---");
            m.write(System.out, "TURTLE");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processNode(Group hdfNode, Resource rdfParent, Model model, Resource typeContainer, Resource typeResource, Property propContains) {
        for (Node child : hdfNode.getChildren().values()) {
            String nodeUri = FILE_NS + child.getPath();
            Resource currentRdfNode = model.createResource(nodeUri);
            rdfParent.addProperty(propContains, currentRdfNode);
            currentRdfNode.addProperty(LWS.partOf, rdfParent);
            switch (child) {
                case Group group -> {
                    currentRdfNode.addProperty(RDF.type, typeContainer);
                    processNode(group, currentRdfNode, model, typeContainer, typeResource, propContains);
                }
                case Dataset dataset -> {
                    currentRdfNode.addProperty(RDF.type, typeResource);
                    Dataset ds = dataset;
                    Resource bnode = currentRdfNode.getModel().createResource();
                    currentRdfNode.addProperty(LWS.representation, bnode);
                    bnode.addLiteral(LWS.sizeInBytes, ds.getSizeInBytes())
                         .addProperty(LWS.mediaType, "application/octet-stream");                    
                }
                default -> {
                }
            }
        }
    }
}
