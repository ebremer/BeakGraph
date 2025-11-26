# BeakGraph<br>
<img
  src="https://github.com/ebremer/BeakGraph/raw/master/beakgraph.png?raw=true"
  width=300px height=300px
  alt="BeakGraph"
  title="BeakGraph"
  style="display: inline-block; margin: 0 auto; max-width: 150px">

## Building

Configuration file generation
```
java -Xmx16G -agentlib:native-image-agent=config-output-dir=src\main\resources\META-INF\native-image -jar target\BeakGraph-0.10.0.jar
```
Native Command-line
```
mvn -Pcmdlinenative clean package
```
Jar Command-line
```
mvn -Pcmdlinejar clean package
```
Core Library Jar Library
```
mvn -Plib clean package
```

## Using BreakGraph in your code

### Creating a BeakGraph from your data
```
HDF5Writer.Builder()
    .setSource(file)
    .setSpatial(true)   # only needed if GeoSPARQL spatial data is present
    .setDestination(dest)
    .build()
    .write();
```

### Using a BeakGraph in your Apache Jena
```
    try (HDF5Reader reader = new HDF5Reader(dest)) {
        BeakGraph bg = new BeakGraph( reader );
        Dataset ds = bg.getDataset();
        ds.getDefaultModel().write(System.out, "NTRIPLE");
    }
```

BeakGraph is a [Apache Jena](https://jena.apache.org/) Graph implementation backed by [HDF5](https://www.hdfgroup.org/solutions/hdf5/).
Beakgraph's HDF5 design is heavily inspired by [HDT](https://www.rdfhdt.org/).

### Author's notes
The first iteration of BeakGraph was backed by Apache Arrow instead of [HDF5](https://www.hdfgroup.org/solutions/hdf5/).  An Apache Arrow version will return.  Reasons for this are varied with some of these reasons being just experimentation.
The general idea of BeakGraph is a read-only, searchable, indexed set of binary [sussinct data structures](https://en.wikipedia.org/wiki/Succinct_data_structure) to represent an [RDF Dataset](https://www.w3.org/TR/rdf11-datasets/).
What these sussinct data structures are stored in, is somewhat immaterial, but the choice of container has it's pro and cons.  HDF5 treats multi-dimensional arrays as first class citizens, and has a free viewer for 
HDF5 files called [HDFView](https://www.hdfgroup.org/download-hdfview/).  HDFView providing a nice way to debug the sussinct data structures during development.  There are other perks to HDF5 which will become apparent in time.

Support for spatial indexing based on [GeoSPARQL](https://github.com/opengeospatial/ogc-geosparql) is being worked on.

The full list of containers under consideration are:
* [HDF5](https://www.hdfgroup.org/solutions/hdf5/)
* [Apache Arrow](https://arrow.apache.org/)
* [Zarr](https://zarr.dev/)
* [Zip](https://en.wikipedia.org/wiki/ZIP_(file_format))
* [DICOM](https://www.dicomstandard.org/)
* [LWS](https://github.com/w3c/lws-protocol)


### Historical
The original BeakGraph was an [Apache Jena](https://jena.apache.org/) Graph implementation backed by [Apache Arrow](https://arrow.apache.org/)
wrapped in a [Research Object Crate (RO-Crate)](https://www.researchobject.org/ro-crate/) inspired by [HDT](https://www.rdfhdt.org/).

Developed to power [Halcyon](https://github.com/halcyon-project/Halcyon).  See [Arxiv](https://arxiv.org/) paper at http://arxiv.org/abs/2304.10612