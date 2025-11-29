package com.ebremer.beakgraph.core.lib;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class HAL {

    /**
     * Halcyon
     * <p>
     * See <a href="https://halcyon.is">Halcyon</a>.
     * <p>
     * <a href="https://halcyon.is/ns/">Base URI and namepace</a>.
     */
    public static final String NS = "https://halcyon.is/ns/";

    // Resources
    public static final Resource Grid = ResourceFactory.createResource(NS + "Grid");
    public static final Resource Heatmap = ResourceFactory.createResource(NS + "Heatmap");
    public static final Resource Segmentation = ResourceFactory.createResource(NS + "Segmentation");
    public static final Resource ColorViewer = ResourceFactory.createResource(NS + "ColorViewer");
    public static final Resource ColorEditor = ResourceFactory.createResource(NS + "ColorEditor");
    public static final Resource SNOMEDEditor = ResourceFactory.createResource(NS + "SNOMEDEditor");
    public static final Resource SHACLForm = ResourceFactory.createResource(NS + "SHACLForm");
    public static final Resource ValidationReport = ResourceFactory.createResource(NS + "ValidationReport");
    public static final Resource AnnotationClass = ResourceFactory.createResource(NS + "AnnotationClass");
    public static final Resource AnnotationClassShape = ResourceFactory.createResource(NS + "AnnotationClassShape");
    public static final Resource AnnotationClassListShape = ResourceFactory.createResource(NS + "AnnotationClassListShape");
    public static final Resource AnnotationClassList = ResourceFactory.createResource(NS + "AnnotationClassList");
    public static final Resource Shapes = ResourceFactory.createResource(NS + "Shapes");
    public static final Resource StorageLocation = ResourceFactory.createResource(NS + "StorageLocation");
    public static final Resource HalcyonSettingsFile = ResourceFactory.createResource(NS + "HalcyonSettingsFile");
    public static final Resource Anonymous = ResourceFactory.createResource(NS + "Anonymous");
    public static final Resource DataFile = ResourceFactory.createResource(NS + "DataFile");
    public static final Resource FileManagerArtifact = ResourceFactory.createResource(NS + "FileManagerArtifact");
    public static final Resource LayerSet = ResourceFactory.createResource(NS + "LayerSet");
    public static final Resource ProbabilityBody = ResourceFactory.createResource(NS + "ProbabilityBody");
    public static final Resource SecurityGraph = ResourceFactory.createResource(NS + "SecurityGraph");
    public static final Resource ImagesAndFeatures = ResourceFactory.createResource(NS + "ImagesAndFeatures");
    public static final Resource GroupsAndUsers = ResourceFactory.createResource(NS + "GroupsAndUsers");
    public static final Resource CollectionsAndResources = ResourceFactory.createResource(NS + "CollectionsAndResources");
    public static final Resource HilbertRange = ResourceFactory.createResource(NS + "HilbertRange");
    public static final Resource Object = ResourceFactory.createResource(NS + "Object");
    public static final Resource HalcyonROCrate = ResourceFactory.createResource(NS + "HalcyonROCrate");
    public static final Resource HalcyonDataset = ResourceFactory.createResource(NS + "HalcyonDataset");
    public static final Resource HalcyonGraph = ResourceFactory.createResource(NS + "HalcyonGraph");
    public static final Resource Feature = ResourceFactory.createResource(NS + "Feature");
    public static final Resource FeatureLayer = ResourceFactory.createResource(NS + "FeatureLayer");
    public static final Resource Predicate = ResourceFactory.createResource(NS + "Predicate");
    public static final Resource Subject = ResourceFactory.createResource(NS + "Subject");
    public static final Resource Collections = ResourceFactory.createResource(NS + "Collections");
    public static final Resource HilbertPolygon = ResourceFactory.createResource(NS + "HilbertPolygon");
    public static final Resource ColorByCertainty = ResourceFactory.createResource(NS + "ColorByCertainty");
    public static final Resource ColorByClassID = ResourceFactory.createResource(NS + "ColorByClassID");
    public static final Resource ColorScheme = ResourceFactory.createResource(NS + "ColorScheme");
    public static final Resource ResourceHandler = ResourceFactory.createResource(NS + "ResourceHandler");
    public static final Resource Annotation = ResourceFactory.createResource(NS + "Annotation");

    // Properties
    public static final Property hilbertCorner = ResourceFactory.createProperty(NS + "hilbertCorner");
    public static final Property hilbertCentroid = ResourceFactory.createProperty(NS + "hilbertCentroid");
    public static final Property annotation = ResourceFactory.createProperty(NS + "annotation");
    public static final Property hasResourceHandler = ResourceFactory.createProperty(NS + "hasResourceHandler");
    public static final Property urlPath = ResourceFactory.createProperty(NS + "urlPath");
    public static final Property resourceBase = ResourceFactory.createProperty(NS + "resourceBase");
    public static final Property fileLastModified = ResourceFactory.createProperty(NS + "fileLastModified");
    public static final Property validFile = ResourceFactory.createProperty(NS + "validFile");
    public static final Property halcyonVersion = ResourceFactory.createProperty(NS + "halcyonVersion");
    public static final Property mode = ResourceFactory.createProperty(NS + "mode");
    public static final Property filemetaversion = ResourceFactory.createProperty(NS + "filemetaversion");
    public static final Property min = ResourceFactory.createProperty(NS + "min");
    public static final Property max = ResourceFactory.createProperty(NS + "max");
    public static final Property asHilbert = ResourceFactory.createProperty(NS + "asHilbert");
    public static final Property hasCreateAction = ResourceFactory.createProperty(NS + "hasCreateAction");
    public static final Property haslayer = ResourceFactory.createProperty(NS + "haslayer");
    public static final Property predicate = ResourceFactory.createProperty(NS + "predicate");
    public static final Property scale = ResourceFactory.createProperty(NS + "scale");
    public static final Property endIndex = ResourceFactory.createProperty(NS + "endIndex");
    public static final Property subject = ResourceFactory.createProperty(NS + "subject");
    public static final Property beginIndex = ResourceFactory.createProperty(NS + "beginIndex");
    public static final Property hasRange = ResourceFactory.createProperty(NS + "hasRange");
    public static final Property hasValue = ResourceFactory.createProperty(NS + "hasValue");
    public static final Property hasAnnotationClass = ResourceFactory.createProperty(NS + "hasAnnotationClass");
    public static final Property measurement = ResourceFactory.createProperty(NS + "measurement");
    public static final Property properties = ResourceFactory.createProperty(NS + "properties");
    public static final Property classification = ResourceFactory.createProperty(NS + "classification");
    public static final Property hasClassification = ResourceFactory.createProperty(NS + "hasClassification");
    public static final Property hasProbability = ResourceFactory.createProperty(NS + "hasProbability");
    public static final Property tileSizeX = ResourceFactory.createProperty(NS + "tileSizeX");
    public static final Property tileSizeY = ResourceFactory.createProperty(NS + "tileSizeY");
    public static final Property scales = ResourceFactory.createProperty(NS + "scales");
    public static final Property scaleIndex = ResourceFactory.createProperty(NS + "scaleIndex");
    public static final Property sorted = ResourceFactory.createProperty(NS + "sorted");
    public static final Property column = ResourceFactory.createProperty(NS + "column");
    public static final Property end = ResourceFactory.createProperty(NS + "end");
    public static final Property object = ResourceFactory.createProperty(NS + "object");
    public static final Property ascending = ResourceFactory.createProperty(NS + "ascending");
    public static final Property gspo = ResourceFactory.createProperty(NS + "gspo");
    public static final Property columns = ResourceFactory.createProperty(NS + "columns");
    public static final Property begin = ResourceFactory.createProperty(NS + "begin");
    public static final Property hasCertainty = ResourceFactory.createProperty(NS + "hasCertainty");
    public static final Property hasFeature = ResourceFactory.createProperty(NS + "hasFeature");
    public static final Property layerNum = ResourceFactory.createProperty(NS + "layerNum");
    public static final Property location = ResourceFactory.createProperty(NS + "location");
    public static final Property opacity = ResourceFactory.createProperty(NS + "opacity");
    public static final Property colorscheme = ResourceFactory.createProperty(NS + "colorscheme");
    public static final Property hasFeatureFile = ResourceFactory.createProperty(NS + "hasFeatureFile");
    public static final Property color = ResourceFactory.createProperty(NS + "color");
    public static final Property cliplow = ResourceFactory.createProperty(NS + "cliplow");
    public static final Property cliphigh = ResourceFactory.createProperty(NS + "cliphigh");
    public static final Property low = ResourceFactory.createProperty(NS + "low");
    public static final Property high = ResourceFactory.createProperty(NS + "high");
    public static final Property colors = ResourceFactory.createProperty(NS + "colors");
    public static final Property classid = ResourceFactory.createProperty(NS + "classid");
    public static final Property colorspectrum = ResourceFactory.createProperty(NS + "colorspectrum");
    public static final Property isSelected = ResourceFactory.createProperty(NS + "isSelected");
    public static final Property webid = ResourceFactory.createProperty(NS + "webid");
    public static final Property assertedClass = ResourceFactory.createProperty(NS + "assertedClass");
    public static final Property hasProperty = ResourceFactory.createProperty(NS + "hasProperty");
    public static final Property RDFStoreLocation = ResourceFactory.createProperty(NS + "RDFStoreLocation");
    public static final Property MasterStorageLocation = ResourceFactory.createProperty(NS + "MasterStorageLocation");
    public static final Property HostName = ResourceFactory.createProperty(NS + "HostName");
    public static final Property HostIP = ResourceFactory.createProperty(NS + "HostIP");
    public static final Property HTTPPort = ResourceFactory.createProperty(NS + "HTTPPort");
    public static final Property HTTPSPort = ResourceFactory.createProperty(NS + "HTTPSPort");
    public static final Property ProxyHostName = ResourceFactory.createProperty(NS + "ProxyHostName");
    public static final Property HTTPS2enabled = ResourceFactory.createProperty(NS + "HTTPS2enabled");
    public static final Property HTTPS3enabled = ResourceFactory.createProperty(NS + "HTTPS3enabled");
    public static final Property SPARQLport = ResourceFactory.createProperty(NS + "SPARQLport");
    public static final Property urlpathprefix = ResourceFactory.createProperty(NS + "urlpathprefix");
    public static final Property mppx = ResourceFactory.createProperty(NS + "mppx");
    public static final Property mppy = ResourceFactory.createProperty(NS + "mppy");
}