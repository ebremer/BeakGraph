package com.ebremer.beakgraph.rdf;

import com.ebremer.rocrate4j.ROCrate;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.compress.archivers.zip.ZipMethod;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;

/**
 *
 * @author erich
 */
public final class BeakWriter {
    private final BufferAllocator allocator = new RootAllocator();
    private final MapDictionaryProvider provider = new MapDictionaryProvider();
    private Dictionary dictionary;
    private final Model m;
    private final CopyOnWriteArrayList<Field> fields = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<FieldVector> vectors = new CopyOnWriteArrayList<>();
    private long bnodes = 0;
    private long numresources = 0;
    private final NodeTable nt;
    private VarCharVector dict;
    private final HashMap<String,PAW> byPredicate = new HashMap<>();
    
    public BeakWriter(Model m, File dest) throws IOException {
        this.m = m;
        long begin = System.nanoTime();
        CreateDictionary();
        nt = new NodeTable(dictionary);
        Create(m);
        System.out.println("# of vectors : "+vectors.size());
        WriteDataToFile(dest);
        File dp = Path.of(dest.toString(),"dictionary").toFile();
        WriteDictionaryToFile(dp);
        DisplayMeta();
        long end = (long) ((System.nanoTime()-begin)/1000000000d);
        System.out.println("Done.  "+end);
    }
    
    public BeakWriter(Model m, ROCrate.Builder roc, String base) throws IOException {
        this.m = m;
        long begin = System.nanoTime();
        CreateDictionary();
        nt = new NodeTable(dictionary);
        Create(m);
        System.out.println("# of vectors : "+vectors.size());
        WriteDataToFile(base, roc);
        WriteDictionaryToFile(base, roc);
        DisplayMeta();
        long end = (long) ((System.nanoTime()-begin)/1000000000d);
        System.out.println("Done.  "+end);
    }
    
    public void closeAll2() {
        vectors.forEach(v->{
            System.out.println("Closing Vector --> "+v.getName());
            v.close();
        });
        
    }
    
    public void DisplayMeta() {
        System.out.println("Displaying Metadata...");    
        vectors.forEach(v->{
            System.out.println(v.getField().getName()+" =====> "+v.getValueCount());
        });
    }
    
    private void CreateDictionary() {
        System.out.println("Creating Dictionary...");
        HashMap<String,Integer> resources = new HashMap<>();
        ResIterator ri = m.listSubjects();
        System.out.println("Scanning Subjects...");
        int count = 0;
        while (ri.hasNext()) {
            count++;
            Resource r = ri.next();
            String rs;
            if (r.isAnon()) {
                bnodes++;
                rs = "_:"+r.toString();
            } else if (r.isResource()) {
                numresources++;
                rs = r.toString();
            } else  {
                throw new Error("What is this?");
            }
            if (rs==null) {
                throw new Error("ack");
            }
            if (!resources.containsKey(rs)) {
                resources.put(rs, 1);
            }
        }
        System.out.println("# of subjects       : "+resources.size());
        System.out.println("# of resources  : "+numresources);
        System.out.println("# of blank nodes    : "+bnodes);
        System.out.println("Scanning Objects...");
        QueryExecution qe = QueryExecutionFactory.create("""
            select distinct ?o where {
                ?s ?p ?o
                filter(isIRI(?o)||isBlank(?o))
            }
            """, m);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();
            Resource r = qs.get("o").asResource();
            String ss;
            if (r.isAnon()) {
                bnodes++;
                ss =  "_:"+r.toString();
            } else if (r.isResource()) {
                numresources++;
                ss = r.getURI();
            } else  {
                throw new Error("What is this?");
            }
            if (!resources.containsKey(ss)) {
                resources.put(ss, 1);
            }   
        }
        System.out.println("# of resources : "+resources.size());
        DictionaryEncoding dictionaryEncoding = new DictionaryEncoding(0, true, new ArrowType.Int(32, true));
        dict = new VarCharVector("Resource Dictionary", allocator);
        dict.allocateNewSafe();
        String[] rrr = resources.keySet().toArray(new String[resources.size()]);
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
       
    public void WriteDataToFile(File file) {
        System.out.println("================== WRITING VECTORS ["+vectors.size()+"] ===================================== ");
        if (!file.exists()) {
            file.mkdirs();
        }
        String base = file.toString();
        vectors.forEach(v->{
            System.out.println("Writing --> "+v.getName());
            File dump = Path.of(base,MD5(v.getField().getName().getBytes())).toFile();
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
            root.setRowCount(v.getValueCount());
            try (                   
                FileOutputStream fos = new FileOutputStream(dump);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            }        
        });
        System.out.println("================== FILE WRITTEN =====================================");
    }
    
    public void WriteDataToFile(String base, ROCrate.Builder roc) {
        System.out.println("================== WRITING VECTORS ["+vectors.size()+"] ===================================== ");
        vectors.forEach(v->{
            System.out.println("Writing --> "+v.getName());
            VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
            root.setRowCount(v.getValueCount());
            try (                   
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
            ) {
                writer.start();
                writer.writeBatch();
                writer.end();
                Resource rde = roc.getRDE();
                Resource target = roc.AddFolder(rde, base, SchemaDO.Dataset);
                roc
                    .Add(target, base, MD5(v.getField().getName().getBytes()), out.toByteArray(), ZipMethod.STORED, true)
                    .addProperty(RDFS.range, ResourceFactory.createResource(v.getName().substring(1,v.getName().length())))
                    .addProperty(RDF.type, ResourceFactory.createResource("https://www.ebremer.com/beakgraph/ns/PredicateVector"));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            }        
        });
        System.out.println("================== FILE WRITTEN =====================================");
    }
    
    public void WriteDictionaryToFile(String base, ROCrate.Builder roc) {
        System.out.println("================== WRITING Dictionary to File =====================================");
        VarCharVector v = (VarCharVector) provider.lookup(0).getVector();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
        try (
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            Resource rde = roc.getRDE();
            Resource target = roc.AddFolder(rde, base, SchemaDO.Dataset);
            roc
                .Add(target, base, "dictionary", out.toByteArray(), ZipMethod.STORED, true)
                .addProperty(RDF.type, ResourceFactory.createResource("https://www.ebremer.com/beakgraph/ns/Dictionary"));
        } catch (IOException ex) {
            Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("================== Dictionary WRITTEN =====================================");
    }
    
    public void WriteDictionaryToFile(File file) {
        System.out.println("================== WRITING Dictionary to File =====================================");
        System.out.println("Writing with this provider : "+provider.getDictionaryIds().size());
        if (!file.getParentFile().exists()) {
            file.mkdirs();
        }
        VarCharVector v = (VarCharVector) provider.lookup(0).getVector();
        VectorSchemaRoot root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
        try (
            FileOutputStream fos = new FileOutputStream(file);
            ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("================== Dictionary WRITTEN =====================================");
    }
    
    public static Model genData() {
        Random random = new Random(1701);    
        Model y = ModelFactory.createDefaultModel();
        for (int c = 0; c<10000; c++) {
            Resource ss = y.createResource("https://www.ebremer.com/ns/id/"+c);
            Property low = y.createProperty("https://www.ebremer.com/ns/low");
            Property high = y.createProperty("https://www.ebremer.com/ns/high");
            y.addLiteral(ss,low,random.nextLong(0,1000L));
            y.addLiteral(ss,high,random.nextLong(0,1000L));
        }
        return y;
    }
    
    public void ProcessTriples() {
        System.out.println("# of triples to be processed : "+m.size());
        ParameterizedSparqlString pss = new ParameterizedSparqlString("select ?s ?p ?o where {?s ?p ?o} #limit 500000");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),m);
        ResultSet rs = qe.execSelect();
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();
            ProcessTriple(qs);
        }
    }
    
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
        
    public void Create(Model src) throws IOException {
        System.out.println("Creating..."); 
        ProcessTriples();
        System.out.println("Closing Source Graph...");
        m.close();
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println(cores+" cores available");
        ThreadPoolExecutor engine = new ThreadPoolExecutor(cores,cores,0L,TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        engine.prestartAllCoreThreads();
        CopyOnWriteArrayList<Future<Model>> list = new CopyOnWriteArrayList<>();
        byPredicate.forEach((k,v)->{
            System.out.println("Submitting -> "+k);
            Callable<Model> worker = new PredicateProcessor(v, fields, vectors);
            list.add(engine.submit(worker));
        });
        System.out.println("All jobs submitted --> "+list.size());
        while ((engine.getQueue().size()+engine.getActiveCount())>0) {
            int c = engine.getQueue().size()+engine.getActiveCount();
            long ram = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024L/1024L;
            System.out.println(engine.isTerminated()+" "+engine.isTerminating()+" jobs completed : "+(list.size()-c)+" remaining jobs: "+c+"  Total RAM used : "+ram+"MB  Maximum RAM : "+(Runtime.getRuntime().maxMemory()/1024L/1024L)+"MB");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(BeakWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
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
            pa.Finish(f,v);
            System.out.println("PP COMPLETE "+pa.getPredicate()+" "+f.size()+" "+v.size());
            return null;
        }
    }
}