package com.ebremer.beakgraph.rdf;

import com.ebremer.rocrate4j.ROCrate;
import com.ebremer.rocrate4j.StopWatch;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import net.lingala.zip4j.model.enums.CompressionMethod;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SchemaDO;

/**
 *
 * @author erich
 */
public final class BeakWriter {
    private final MapDictionaryProvider provider = new MapDictionaryProvider();
    private Dictionary dictionary;
    private final Model m;
    private final CopyOnWriteArrayList<Field> fields = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<FieldVector> vectors = new CopyOnWriteArrayList<>();
    private final NodeTable nt;
    private LargeVarCharVector dict;
    private final Resource metairi;
    private final HashMap<String,PAW> byPredicate = new HashMap<>();
    private final HashMap<String,Integer> blanknodes;
    private final ConcurrentHashMap<String,Job> Jobs = new ConcurrentHashMap<>();
    
    public BeakWriter(BufferAllocator allocator, Model m, ROCrate.Builder roc, String base) throws IOException {
        this.m = m;
        StopWatch sw = new StopWatch();
        sw.LapStart("Create Dictionary");
        blanknodes = new HashMap<>(2500000);
        CreateDictionary(allocator);
        nt = new NodeTable(dictionary);
        nt.setBlankNodes(blanknodes);
        sw.Lap("Dictionary Created");
        sw.LapStart("Create Predicate Vectors");
        Create(allocator);
        System.out.println("# of vectors : "+vectors.size());
        sw.Lap("Predicate Vectors Created");
        metairi = WriteDictionaryToFile(base, roc);
        sw.LapStart("Generate VoID Data");
        Model VoID = BGVoID.GenerateVoID(metairi, m);
        metairi.getModel().add(VoID);
        sw.Lap("VoID Data Generated");
        WriteDataToFile(base, roc);
        //DisplayMeta();
        sw.Lapse("BeakGraph Completed");
    }
    
    public Resource getMetaIRI() {
        return metairi;
    }
        
    public void DisplayMeta() {
        System.out.println("Displaying Metadata...");    
        vectors.forEach(v->{
            System.out.println(v.getField().getName()+" =====> "+v.getValueCount());
        });
    }
    
    private void CreateDictionary(BufferAllocator allocator) {
        HashMap<String,Integer> resources = new HashMap<>();
        StmtIterator si = m.listStatements();
        while (si.hasNext()) {
            Statement sx = si.next();
            Resource r = sx.getSubject();
            String rs;
            if (r.isAnon()) {
                rs = "_:"+r.toString();
                if (!blanknodes.containsKey(rs)) {
                   blanknodes.put(rs, -(blanknodes.size()+1));
                }
            } else if (r.isResource()) {
                rs = r.toString();
                if (!resources.containsKey(rs)) {
                    resources.put(rs, 1);
                }
            }
            RDFNode node = sx.getObject();
            if (node.isAnon()) {
                rs = "_:"+node.toString();
                if (!blanknodes.containsKey(rs)) {
                    blanknodes.put(rs, -(blanknodes.size()+1));
                }
            } else if (node.isResource()) {
                rs = node.asResource().toString();
                if (!resources.containsKey(rs)) {
                    resources.put(rs, 1);
                }
            }
        }
        System.out.println("# of blank nodes    : " + blanknodes.size());
        System.out.println("# of resources      : " + resources.size());
        DictionaryEncoding dictionaryEncoding = new DictionaryEncoding(0, true, new ArrowType.Int(32, true));
        dict = new LargeVarCharVector("Resource Dictionary", allocator);
        dict.allocateNewSafe();
        String[] rrr = resources.keySet().toArray(new String[resources.size()]);
        System.out.println("Sorting Dictionary...");
        Arrays.sort(rrr);
        for (int i=0; i<resources.size(); i++) {
            dict.setSafe(i, rrr[i].getBytes(StandardCharsets.UTF_8));
        }
        dict.setValueCount(resources.size());
        System.out.println("FINAL # : "+resources.size());
        dictionary = new Dictionary(dict, dictionaryEncoding);
        System.out.println(dictionary.getEncoding());
        provider.put(dictionary);
        System.out.println("Creating Dictionary...Done");
    }
    
    public String MD5(byte[] buffer) {      
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(buffer);
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toUpperCase();
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public Resource WriteDataToFile(String base, ROCrate.Builder roc) {
        System.out.println("================== WRITING VECTORS ["+vectors.size()+"] ===================================== ");
        Resource rde = roc.getRDE();
        Resource target = roc.AddFolder(rde, base, BG.BeakGraph);
        vectors.forEach(v->{
            System.out.println("Writing --> "+v.getName());
            try (
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
            )
            {
                root.setRowCount(v.getValueCount());
             //   OutputStream zos = roc.getDestination().GetOutputStream(base+"/"+MD5(v.getName().getBytes()), CompressionMethod.STORE);
              //  CountingOutputStream cos = new CountingOutputStream(zos);
            //    ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(cos));
                writer.start();
                writer.writeBatch();
                writer.end();
            //    long numbytes = cos.getNumberOfBytesWritten();
//                if (zos instanceof ZipOutputStream zz) {
  //                  FileHeader fh = zz.closeEntry();
    //                fh.setUncompressedSize(numbytes);
      //          }
                roc
                    .Add(target, base, MD5(v.getField().getName().getBytes()), out.toByteArray(), CompressionMethod.STORE, true)
                    //.Add(target, base, MD5(v.getField().getName().getBytes()), CompressionMethod.STORE, true)
                    .addProperty(SchemaDO.name, v.getField().getName().substring(1))
                    .addProperty(BG.property, ResourceFactory.createResource(v.getName().substring(1,v.getName().length())))
                    .addProperty(SchemaDO.encodingFormat, "application/vnd.apache.arrow.file")
                    //.addLiteral(SchemaDO.contentSize, numbytes)
                    .addLiteral(SchemaDO.contentSize, out.size())
                    .addProperty(RDF.type, SchemaDO.MediaObject)
                    .addLiteral(BG.triples, v.getValueCount())
                    .addProperty(RDF.type, BG.PredicateVector);
             //   writer.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            }        
        });
        System.out.println("================== FILE WRITTEN =====================================");
        return target;
    }
    
    public Resource WriteDictionaryToFile(String base, ROCrate.Builder roc) {
        System.out.println("================== WRITING Dictionary to File =====================================");
        Resource rde = roc.getRDE();
        Resource target = roc.AddFolder(rde, base, SchemaDO.Dataset);
        try (
            LargeVarCharVector v = (LargeVarCharVector) provider.lookup(0).getVector();
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
          //  ZipOutputStream zos = roc.getDestination().GetOutputStream("dictionary", CompressionMethod.STORE);
           // CountingOutputStream cos = new CountingOutputStream(zos);
           // ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(cos))
        ) {
        //try {
          //  OutputStream zos = roc.getDestination().GetOutputStream(base+"/dictionary", CompressionMethod.STORE);
           // CountingOutputStream cos = new CountingOutputStream(zos);
     //       ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(cos));
            writer.start();
            writer.writeBatch();
            writer.end();
            //long numbytes = cos.getNumberOfBytesWritten();
            //if (zos instanceof ZipOutputStream zz) {
              //      FileHeader fh = zz.closeEntry();
//                    fh.setUncompressedSize(numbytes);
  //              }
            roc
                .Add(target, base, "dictionary", out.toByteArray(), CompressionMethod.STORE, true)
                //.Add(target, base, "dictionary", CompressionMethod.STORE, true)
                .addProperty(SchemaDO.encodingFormat, "application/vnd.apache.arrow.file")
                //.addLiteral(SchemaDO.contentSize, numbytes)
                .addLiteral(SchemaDO.contentSize, out.size())    
                .addProperty(RDF.type, SchemaDO.MediaObject)
                .addProperty(RDF.type, BG.Dictionary);
            /*
                        roc
                .Add(target, base, "dictionary", out.toByteArray(), CompressionMethod.STORE, true)
                .addProperty(SchemaDO.encodingFormat, "application/vnd.apache.arrow.file")
                .addLiteral(SchemaDO.contentSize, out.size())
                .addProperty(RDF.type, SchemaDO.MediaObject)
                .addProperty(RDF.type, BG.Dictionary);
            */
           //writer.close();
        } catch (IOException ex) {
            Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("================== Dictionary WRITTEN =====================================");
        return target;
    }
    
    public void ProcessTriples(BufferAllocator allocator) {
        System.out.println("# of triples to be processed : "+m.size());
        StmtIterator si = m.listStatements();
        while (si.hasNext()) {
            ProcessTriple2(allocator, si.next());
        }
    }
    
    public void ProcessTriple2(BufferAllocator allocator, Statement stmt) {
        //Resource res = qs.get("s").asResource();
        Resource res = stmt.getSubject();
        String s;
        if (res.isAnon()) {
            s = "_:"+res.toString();
        } else if (res.isResource()) {
            s = res.getURI();
        } else {
            throw new Error("ack");
        }
        String p = stmt.getPredicate().asResource().getURI();
        Class cc;
        RDFNode o = stmt.getObject();
        if (o.isResource()) {
            cc = o.asResource().getClass();
        } else if (o.isAnon()) {
            cc = o.asResource().getClass();
        } else if (o.isLiteral()) {
            cc = o.asLiteral().getDatatype().getJavaClass();
        } else {
            Resource r = o.asResource();
            throw new Error(r.isResource()+" Don't know how to deal with "+o.toString());
        }
        String ct = cc.getCanonicalName();
        if (null == ct) {
            System.out.println("Can't handle ["+ct+"]");
            System.out.println(s+" "+p+" "+o);
        } else switch (ct) {
            case "org.apache.jena.rdf.model.impl.ResourceImpl": {
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, o.asResource());
                break;
            }
            case "java.math.BigInteger": {
                long oo = o.asLiteral().getLong();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Integer": {
                int oo = o.asLiteral().getInt();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Long": {
                long oo = o.asLiteral().getLong();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Float": {
                float oo = o.asLiteral().getFloat();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.String": {
                String oo = o.asLiteral().toString();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            default:
                System.out.println("Can't handle ["+ct+"]");
                System.out.println(s+" "+p+" "+o);
                throw new Error("BAMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM");
                //break;
        }
    }
    
    public void Create(BufferAllocator allocator) throws IOException {
        System.out.println("Creating..."); 
        ProcessTriples(allocator);
       // System.out.println("Closing Source Graph...");
       // m.close();
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println(cores+" cores available");
        ThreadPoolExecutor engine = new ThreadPoolExecutor(cores,cores,0L,TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        CopyOnWriteArrayList<Future<Model>> list = new CopyOnWriteArrayList<>();
        byPredicate.forEach((k,v)->{
            System.out.println("Submitting -> "+k);
            Callable<Model> worker = new PredicateProcessor(v, fields, vectors);
            list.add(engine.submit(worker));
            Jobs.put(k, new Job(k,worker,"WAITING"));
        });
        System.out.println("All jobs submitted --> "+list.size());
        engine.prestartAllCoreThreads();
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int c = engine.getQueue().size()+engine.getActiveCount();
                long ram = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024L/1024L;
                System.out.println("===============================================\njobs completed : "+(list.size()-c)+" remaining jobs: "+c+"  Total RAM used : "+ram+"MB  Maximum RAM : "+(Runtime.getRuntime().maxMemory()/1024L/1024L)+"MB");
                Jobs.forEach((k,v)->{
                    if (!"DONE".equals(v.status)) {
                        System.out.println(v.predicate+" ---> "+v.status);
                    }
                });
            }
        }, 0, 10000);
        while ((engine.getQueue().size()+engine.getActiveCount())>0) {
//            int c = engine.getQueue().size()+engine.getActiveCount();
  //          long ram = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024L/1024L;
    //        System.out.println("===============================================\njobs completed : "+(list.size()-c)+" remaining jobs: "+c+"  Total RAM used : "+ram+"MB  Maximum RAM : "+(Runtime.getRuntime().maxMemory()/1024L/1024L)+"MB");
      //      Jobs.forEach((k,v)->{
        //        if (!"DONE".equals(v.status)) {
          //          System.out.println(v.predicate+" ---> "+v.status);
            //    }
            //});
            //try {
//                Thread.sleep(10000);
           // } catch (InterruptedException ex) {
             //   Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            //}
        }
        timer.cancel();
        System.out.println("Engine shutdown");
        engine.shutdown();
        System.out.println("engine jobs : "+list.size());
    }
        
    class PredicateProcessor implements Callable<Model> {
        private final PAW pa;
        private final CopyOnWriteArrayList f;
        private final CopyOnWriteArrayList v;
        
        public PredicateProcessor(PAW pa, CopyOnWriteArrayList f, CopyOnWriteArrayList v) {
            this.pa = pa;
            this.v = v;
            this.f = f;
        }

        @Override
        public Model call() {
            Job job = Jobs.get(pa.getPredicate());
            job.status = "START";
            pa.Finish(f,v,job);
            job.status = "DONE";
            return null;
        }
    }
}

    /*
    
    public void ProcessTriple(QuerySolution qs) {
        Resource res = qs.get("s").asResource();
        String s;
        if (res.isAnon()) {
            s = "_:"+res.toString();
        } else if (res.isResource()) {
            s = res.getURI();
        } else {
            throw new Error("ack");
        }
        String p = qs.get("p").asResource().getURI();
        Class cc;
        RDFNode o = qs.get("o");
        if (o.isResource()) {
            cc = o.asResource().getClass();
        } else if (o.isAnon()) {
            cc = o.asResource().getClass();
        } else if (o.isLiteral()) {
            cc = o.asLiteral().getDatatype().getJavaClass();
        } else {
            Resource r = o.asResource();
            throw new Error(r.isResource()+" Don't know how to deal with "+o.toString());
        }
        String ct = cc.getCanonicalName();
        if (null == ct) {
            System.out.println("Can't handle ["+ct+"]");
            System.out.println(s+" "+p+" "+o);
        } else switch (ct) {
            case "org.apache.jena.rdf.model.impl.ResourceImpl": {
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, o.asResource());
                break;
            }
            case "java.math.BigInteger": {
                long oo = o.asLiteral().getLong();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Integer": {
                int oo = o.asLiteral().getInt();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Long": {
                long oo = o.asLiteral().getLong();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.Float": {
                float oo = o.asLiteral().getFloat();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            case "java.lang.String": {
                String oo = o.asLiteral().toString();
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAW(allocator, nt, p));
                }
                byPredicate.get(p).set(s, oo);
                break;
            }
            default:
                System.out.println("Can't handle ["+ct+"]");
                System.out.println(s+" "+p+" "+o);
                throw new Error("BAMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM");
                //break;
        }
    }
        */

        /*
        ParameterizedSparqlString pss = new ParameterizedSparqlString("select ?s ?p ?o where {?s ?p ?o} #limit 500000");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),m);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();
            ProcessTriple(qs);
        }*/