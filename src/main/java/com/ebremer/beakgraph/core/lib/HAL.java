package com.ebremer.beakgraph.core.lib;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class HAL {

/**
 *  Halcyon
 *  <p>
 *	See <a href="https://halcyon.is">Halcyon</a>.
 *  <p>
 *  <a href="https://halcyon.is/ns/">Base URI and namepace</a>.
 */
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "https://halcyon.is/ns/";
    
    public static final Resource Grid = m.createResource( NS + "Grid" );
    public static final Resource Heatmap  = m.createResource( NS + "Heatmap" );
    public static final Resource Segmentation  = m.createResource( NS + "Segmentation" );
    public static final Resource ColorViewer  = m.createResource( NS + "ColorViewer" );
    public static final Resource ColorEditor  = m.createResource( NS + "ColorEditor" );
    public static final Resource SNOMEDEditor = m.createResource(NS+"SNOMEDEditor");
    public static final Resource SHACLForm = m.createResource(NS+"SHACLForm");
    public static final Resource ValidationReport = m.createResource(NS+"ValidationReport");
    public static final Resource AnnotationClass = m.createResource(NS+"AnnotationClass");
    public static final Resource AnnotationClassShape = m.createResource(NS+"AnnotationClassShape");
    public static final Resource AnnotationClassListShape = m.createResource(NS+"AnnotationClassListShape");
    public static final Resource AnnotationClassList = m.createResource(NS+"AnnotationClassList");
    public static final Resource Shapes = m.createResource(NS+"Shapes");
    public static final Resource StorageLocation = m.createResource(NS+"StorageLocation");
    public static final Resource HalcyonSettingsFile = m.createResource(NS+"HalcyonSettingsFile");
    public static final Resource Anonymous = m.createResource(NS+"Anonymous");  // Identifier for a group of unidentified things
    public static final Resource DataFile = m.createResource(NS+"DataFile");
    public static final Resource FileManagerArtifact = m.createResource(NS+"FileManagerArtifact");
    public static final Resource LayerSet = m.createResource(NS+"LayerSet");
    public static final Resource ProbabilityBody = m.createResource(NS+"ProbabilityBody");
    public static final Resource SecurityGraph = m.createResource(NS+"SecurityGraph");
    public static final Resource ImagesAndFeatures = m.createResource(NS+"ImagesAndFeatures");
    public static final Resource GroupsAndUsers = m.createResource(NS+"GroupsAndUsers");
    public static final Resource CollectionsAndResources = m.createResource(NS+"CollectionsAndResources");
    public static final Resource HilbertRange = m.createResource(NS+"HilbertRange");
    public static final Resource Object = m.createResource(NS+"Object");;
    public static final Resource HalcyonROCrate = m.createResource(NS+"HalcyonROCrate");
    public static final Resource HalcyonDataset = m.createResource(NS+"HalcyonDataset");
    public static final Resource HalcyonGraph = m.createResource(NS+"HalcyonGraph");
    public static final Resource Feature = m.createResource(NS+"Feature");
    public static final Resource FeatureLayer = m.createResource(NS+"FeatureLayer");
    public static final Resource Predicate = m.createResource(NS+"Predicate");
    public static final Resource Subject = m.createResource(NS+"Subject");
    public static final Resource Collections = m.createResource(NS+"Collections");
    public static final Resource HilbertPolygon = m.createResource(NS+"HilbertPolygon");
    public static final Resource ColorByCertainty = m.createResource(NS+"ColorByCertainty");
    public static final Resource ColorByClassID = m.createResource(NS+"ColorByClassID");
    public static final Resource ColorScheme = m.createResource(NS+"ColorScheme");
    public static final Resource ResourceHandler = m.createResource(NS+"ResourceHandler");
    public static final Resource Annotation = m.createResource(NS+"Annotation");
    
    public static final Property annotation = m.createProperty(NS+"annotation");
    public static final Property hasResourceHandler = m.createProperty(NS+"hasResourceHandler");
    public static final Property urlPath = m.createProperty(NS+"urlPath");
    public static final Property resourceBase = m.createProperty(NS+"resourceBase");
    public static final Property fileLastModified = m.createProperty(NS+"fileLastModified");
    public static final Property validFile = m.createProperty(NS+"validFile");
    public static final Property halcyonVersion = m.createProperty(NS+"halcyonVersion");
    public static final Property mode = m.createProperty(NS+"mode");
    public static final Property filemetaversion = m.createProperty(NS+"filemetaversion");
    public static final Property min = m.createProperty(NS+"min");
    public static final Property max = m.createProperty(NS+"max");
    public static final Property asHilbert = m.createProperty(NS+"asHilbert");
    public static final Property hasCreateAction = m.createProperty(NS+"hasCreateAction");
    public static final Property haslayer = m.createProperty(NS+"haslayer");
    public static final Property predicate = m.createProperty(NS+"predicate");
    public static final Property scale = m.createProperty(NS+"scale");
    public static final Property endIndex = m.createProperty(NS+"endIndex");
    public static final Property subject = m.createProperty(NS+"subject");
    public static final Property beginIndex = m.createProperty(NS+"beginIndex");
    public static final Property hasRange = m.createProperty(NS+"hasRange");
    public static final Property hasValue = m.createProperty(NS+"hasValue");
    public static final Property hasAnnotationClass = m.createProperty(NS+"hasAnnotationClass");
    public static final Property measurement = m.createProperty(NS+"measurement");
    public static final Property properties = m.createProperty(NS+"properties");
    public static final Property classification = m.createProperty(NS+"classification");
    public static final Property hasClassification = m.createProperty(NS+"hasClassification");
    public static final Property hasProbability = m.createProperty(NS+"hasProbability");
    public static final Property tileSizeX = m.createProperty(NS+"tileSizeX");
    public static final Property tileSizeY = m.createProperty(NS+"tileSizeY");
    public static final Property scales = m.createProperty(NS+"scales");
    public static final Property scaleIndex = m.createProperty(NS+"scaleIndex");
    public static final Property sorted = m.createProperty(NS+"sorted");
    public static final Property column = m.createProperty(NS+"column");
    public static final Property end = m.createProperty(NS+"end");
    public static final Property object = m.createProperty(NS+"object");
    public static final Property ascending = m.createProperty(NS+"ascending");
    public static final Property gspo = m.createProperty(NS+"gspo");
    public static final Property columns = m.createProperty(NS+"columns");
    public static final Property begin = m.createProperty(NS+"begin");
    public static final Property hasCertainty = m.createProperty(NS+"hasCertainty");
    public static final Property hasFeature = m.createProperty(NS+"hasFeature");
    public static final Property layerNum = m.createProperty(NS+"layerNum");
    public static final Property location = m.createProperty(NS+"location");
    public static final Property opacity = m.createProperty(NS+"opacity");
    public static final Property colorscheme = m.createProperty(NS+"colorscheme");
    public static final Property hasFeatureFile = m.createProperty(NS+"hasFeatureFile");
    public static final Property color = m.createProperty(NS+"color");
    public static final Property cliplow = m.createProperty(NS+"cliplow");
    public static final Property cliphigh = m.createProperty(NS+"cliphigh");
    public static final Property low = m.createProperty(NS+"low");
    public static final Property high = m.createProperty(NS+"high");
    public static final Property colors = m.createProperty(NS+"colors");
    public static final Property classid = m.createProperty(NS+"classid");
    public static final Property colorspectrum = m.createProperty(NS+"colorspectrum");
    public static final Property isSelected = m.createProperty(NS+"isSelected");
    public static final Property webid = m.createProperty(NS+"webid");
    public static final Property assertedClass = m.createProperty(NS+"assertedClass");
    public static final Property hasProperty = m.createProperty(NS+"hasProperty");
    public static final Property RDFStoreLocation = m.createProperty(NS+"RDFStoreLocation");
    public static final Property MasterStorageLocation = m.createProperty(NS+"MasterStorageLocation");
    public static final Property HostName = m.createProperty(NS+"HostName");
    public static final Property HostIP = m.createProperty(NS+"HostIP");
    public static final Property HTTPPort = m.createProperty(NS+"HTTPPort");
    public static final Property HTTPSPort = m.createProperty(NS+"HTTPSPort");
    public static final Property ProxyHostName = m.createProperty(NS+"ProxyHostName");
    public static final Property HTTPS2enabled = m.createProperty(NS+"HTTPS2enabled");
    public static final Property HTTPS3enabled = m.createProperty(NS+"HTTPS3enabled");
    public static final Property SPARQLport = m.createProperty(NS+"SPARQLport");
    public static final Property urlpathprefix = m.createProperty(NS+"urlpathprefix");
    public static final Property mppx = m.createProperty(NS+"mppx");
    public static final Property mppy = m.createProperty(NS+"mppy");
}
