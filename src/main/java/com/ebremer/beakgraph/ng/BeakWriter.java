package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.BeakGraph.DICTIONARY;
import static com.ebremer.beakgraph.ng.BeakGraph.NAMEDGRAPHS;
import static com.ebremer.beakgraph.ng.DataType.FLOAT;
import static com.ebremer.beakgraph.ng.DataType.LONG;
import static com.ebremer.beakgraph.ng.DataType.STRING;
import com.ebremer.rocrate4j.ROCrate.ROCrateBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.lingala.zip4j.model.enums.CompressionMethod;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.vocabulary.SchemaDO;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public final class BeakWriter implements AutoCloseable {
    private final NodeTable nt;
    private final BufferAllocator allocator;
    private final CopyOnWriteArrayList<FieldVector> vectors = new CopyOnWriteArrayList<>();
    private final HashMap<String,PAW> byPredicate = new HashMap<>();
    private final HashMap<Node,Integer> blanknodes;
    private final String base;
    private final BGVoID VoID = new BGVoID();
    private final ConcurrentHashMap<String,Writer> writers;
    private final ROCrateBuilder roc;
    private VarCharVector IRI2idx;
    private IntVector idx2id;
    private VarCharVector id2IRI;
    private IntVector id2ng;
    private IntVector ng2id;
    private List<SpecialProcess> specials;
    private final Resource target;    
    private static final Logger logger = LoggerFactory.getLogger(BeakWriter.class);
    
    public BeakWriter(ROCrateBuilder roc, String base) {
        this.allocator = new RootAllocator();
        this.base = base;
        this.roc = roc;
        blanknodes = new HashMap<>(2500000);
        writers = new ConcurrentHashMap<>();
        nt = new NodeTable();
        nt.setBlankNodes(blanknodes);
        target = roc.AddFolder(this.roc.getRDE(), this.base, BG.BeakGraph);
    }
    
    public Resource getTarget() {
        return target;
    }
    
    public HashMap<String,PAW> getbyPredicate() {
        return byPredicate;
    }
    
    public String getBase() {
        return base;
    }
    
    public void HandleThese(List<BG.PropertyAndDataType> pairs) {
        pairs.forEach(pair -> {
            if (!byPredicate.containsKey(pair.predicate())) {
                byPredicate.put(pair.predicate(), new PAW(allocator.newChildAllocator("PAW -> "+pair.predicate(), 0, Long.MAX_VALUE), nt, pair.predicate()));
            }
            PAW paw = byPredicate.get(pair.predicate());
            paw.Handle(pair.dataType());
        });
    }
    
    public ROCrateBuilder getROC() {
        return roc;
    }
    
    public void setSpecials(List<SpecialProcess> specials) {
        this.specials = specials;
    }
        
    public void Analyze(Dataset ds) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select distinct ?p ?datatype
            where {
                {
                    ?s ?p ?o
                } union {
                    graph ?g {?s ?p ?o}
                }
                bind(datatype(?o) as ?datatype)
            }
            """
        );
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),ds);
        ResultSet rs = qe.execSelect();
        rs.forEachRemaining(qs->{
            String p = qs.get("p").asResource().getURI();
            if (!byPredicate.containsKey(p)) {
                byPredicate.put(p, new PAW(allocator.newChildAllocator("PAW -> "+p, 0, Long.MAX_VALUE), nt, p));
            }
            PAW paw = byPredicate.get(p);
            RDFNode dt = qs.get("datatype");
            if (dt==null) {
                paw.Handle(null);
            } else {
                paw.Handle(dt.asResource());
            }
        });
        byPredicate.forEach((k,paw)->{
            paw.getCS().forEach((dt,sv)->{
                vectors.add(sv);
            });
            paw.resetCounts();
        });
        Model mk = ModelFactory.createDefaultModel();
        Resource b = mk.createResource();
        byPredicate.forEach((k,paw)->{
            Property pk = mk.createProperty(k);
            paw.getCS().forEach((dt,sv)->{
                switch (dt) {
                    case STRING -> ProcessTriple(mk.createStatement(b, pk, "1"),false);
                    case RESOURCE -> ProcessTriple(mk.createStatement(b, pk, b),false);
                    case FLOAT -> ProcessTriple(mk.createLiteralStatement(b, pk, 1.0f),false);
                    case INTEGER -> ProcessTriple(mk.createLiteralStatement(b, pk, 1),false);
                    case LONG -> ProcessTriple(mk.createLiteralStatement(b, pk, 1L),false);
                    case DOUBLE -> ProcessTriple(mk.createLiteralStatement(b, pk, 1.0d),false);
                }
            });
            paw.Finish(this);
            paw.resetCounts();
        });
    }

    public void Add(Dataset ds) {
        Analyze(ds);
        Resource dg = ResourceFactory.createResource("urn:halcyon:defaultgraph");
        RegisterNamedGraph(dg);
        Add(dg, ds.getDefaultModel());
        int c = 0;
        Iterator<Resource> ngs = ds.listModelNames();
        while (ngs.hasNext()) {
            ngs.next();
            c++;
        }
        ngs = ds.listModelNames();
        while (ngs.hasNext()) {
            StopWatch sw = StopWatch.getInstance();
            Resource ng = ngs.next();
            byPredicate.forEach((k,paw)->{
                paw.sumCounts();
                paw.resetCounts();
                paw.resetVectors();
            });
            nt.AddNamedGraph(ng.asNode());
            Add(ng,ds.getNamedModel(ng));
            c--;
            sw.Lapse(ng+" "+c);
        }
    }
    
    public void RegisterNamedGraph(Resource ng) {
        nt.AddNamedGraph(ng.asNode());
    }
    
    public void Add(Resource ng, Model m) {
        logger.debug("Add Named Graph");
        StopWatch sw = StopWatch.getInstance();
        specials.forEach(sp->{
            sp.Execute(this, ng, m);
        });
        logger.debug(sw.Lapse("Time for specials"));        
        sw.reset();
        m.listStatements().forEach(s->{
            ProcessTriple(s);
        });
        logger.debug(sw.Lapse("Time to Process Triples"));          
        sw.reset();
        AddModel();
    }

    private VectorSchemaRoot CreateNGDictionary() {
        HashMap<Node,Integer> ha = nt.getNGResources();
        record Pair(int ng, int id) {}
        int size = ha.size();
        ArrayList<Pair> d = new ArrayList<>(size);
        ha.forEach((k,v)->{
            d.add(new Pair(v,nt.getID(k)));
        });
        d.sort((Pair p1, Pair p2) -> p1.ng() - p2.ng());
        ng2id = new IntVector("ng2id", allocator);
        ng2id.allocateNew(size);
        Iterator<Pair> rs = d.iterator();
        int u=0;
        while (rs.hasNext()) {
            Pair r = rs.next();
            ng2id.setSafe(r.ng(),r.id());
            u++;
        }
        ng2id.setValueCount(size);
        List<Field> f = Arrays.asList(ng2id.getField());
        List<FieldVector> v = Arrays.asList(ng2id);
        VectorSchemaRoot root = new VectorSchemaRoot(f, v);
        return root;
    }
    
    private VectorSchemaRoot CreateDictionary() {
        HashMap<Node,Integer> ha = nt.getResources();
        record Trio(String iri, int index, Integer ng) {}
        ArrayList<Trio> d = new ArrayList<>(ha.size());
        ha.forEach((k,v)->{
            d.add(new Trio(k.getURI(),v,nt.getNGResources().get(k)));
        });        
        IRI2idx = new VarCharVector("IRI2idx", allocator);
        idx2id = new IntVector("idx2id", allocator);
        id2IRI = new VarCharVector("id2IRI", allocator);
        id2ng = new IntVector("id2ng", allocator);        
        idx2id.allocateNew(ha.size());
        IRI2idx.allocateNew(ha.size());
        id2IRI.allocateNew(ha.size());
        id2ng.allocateNew(ha.size());        
        d.sort((Trio p1, Trio p2) -> p1.iri.compareTo(p2.iri()));
        Iterator<Trio> rs = d.iterator();
        int u=0;
        while (rs.hasNext()) {
            Trio r = rs.next();
            IRI2idx.setSafe(u, new Text(r.iri()));
            idx2id.setSafe(u,r.index());
            u++;
        }
        d.sort((Trio p1, Trio p2) -> p1.index()-p2.index());
        rs = d.iterator();
        u=0;
        while (rs.hasNext()) {
            Trio r = rs.next();
            id2IRI.setSafe(r.index(), new Text(r.iri()));
            if (r.ng()==null) {
                id2ng.setNull(r.index());
            } else {
                id2ng.setSafe(r.index(), r.ng());
            }
            u++;
        }
        idx2id.setValueCount(d.size());
        IRI2idx.setValueCount(d.size());
        id2IRI.setValueCount(d.size());
        id2ng.setValueCount(d.size());
        List<Field> f = Arrays.asList(IRI2idx.getField(), idx2id.getField(), id2IRI.getField(), id2ng.getField());
        List<FieldVector> v = Arrays.asList(IRI2idx, idx2id, id2IRI, id2ng);
        VectorSchemaRoot root = new VectorSchemaRoot(f, v);
        return root;
    }

    public void WriteNGDictionaryToFile() {        
        try (
            VectorSchemaRoot root = CreateNGDictionary();
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*1024*1024*100);
         ) {
            try {
                Map<String, String> meta = new HashMap<>();
                meta.put("version", "2.0.0");
                try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(bos), meta, IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD)) {  
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    byte[] buffer = bos.toByteArray();
                    roc.getROCrateWriter().Add(base+"/"+NAMEDGRAPHS, buffer, CompressionMethod.STORE);
                    roc
                        .Add(target, base, NAMEDGRAPHS, CompressionMethod.STORE, true);
                        //.addProperty(SchemaDO.encodingFormat, "application/vnd.apache.arrow.file")
                        //.addLiteral(SchemaDO.contentSize, buffer.length)
                        //.addProperty(RDF.type, BG.NamedGraphs);
                    }
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }   catch (IOException ex) {
            logger.error(ex.getMessage());
        }
    }
    
    public void WriteDictionaryToFile() {        
        try (
            VectorSchemaRoot root = CreateDictionary();
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*1024*1024*100);
        ) {
            try {
                Map<String, String> meta = new HashMap<>();
                meta.put("version", "2.0.0");
                try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(bos), meta, IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD)) {  
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    byte[] buffer = bos.toByteArray();
                    roc.getROCrateWriter().Add(base+"/"+DICTIONARY, buffer, CompressionMethod.STORE);
                    roc
                        .Add(target, base, DICTIONARY, CompressionMethod.STORE, true);
                        //.addProperty(SchemaDO.encodingFormat, "application/vnd.apache.arrow.file")
                        //.addLiteral(SchemaDO.contentSize, buffer.length)
                        //.addProperty(RDF.type, BG.Dictionary);
                    }
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
    }
    
    public Writer getWriter(String v) {
        if (!writers.containsKey(v)) {
            writers.put(v, new Writer(base));
        }
        return writers.get(v);
    }
    
    public void AddModel() {        
        StopWatch sw = StopWatch.getInstance();
        try (ExecutorService engine = Executors.newVirtualThreadPerTaskExecutor()) {
            byPredicate.forEach((p,paw)->{
                logger.trace("submit PredicateProcessor -> "+p);
                engine.submit(new PredicateProcessor(paw, this));
            });
        }
        logger.debug(sw.Lapse("Time to Finalized Model"));
        sw.reset();
        try (ExecutorService engine = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int w=0; w<vectors.size(); w++) {
                FieldVector v = vectors.get(w);
                Writer block = getWriter(v.getName());
                block.getVectorSchemaRoot().setRowCount(v.getValueCount());
                engine.submit(new WB(block));
            }
        }
        logger.debug(sw.Lapse("Time for Writers"));
    }

    private class WB implements Runnable {
        private final Writer block;
        
        public WB(Writer block) {
            this.block = block;
        }

        @Override
        public void run() {
            try {
                block.getWriter().writeBatch();
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }
    }
    
    public void ProcessTriple(Statement stmt) {
        logger.trace("ProcessTriple : "+stmt.toString());
        ProcessTriple(stmt, true);
    }
    
    public void ProcessTriple(Statement stmt, boolean track) {
        if (track) VoID.Add(stmt);
        Resource res = stmt.getSubject();
       if (res.isAnon()) {
            if (!nt.getBlankNodes().containsKey(res.asNode())) {
                nt.getBlankNodes().put(res.asNode(), blanknodes.size()-Integer.MIN_VALUE);
            }
        } else if (res.isResource()) {           
        } else {
            throw new Error("ack");
        }
        String p = stmt.getPredicate().asResource().getURI();
        Class<?> cc;
        RDFNode o = stmt.getObject();
        if (o.isResource()) {
            cc = o.asResource().getClass();
            if (o.isAnon()) {
                if (!nt.getBlankNodes().containsKey(o.asResource().asNode())) {
                    nt.getBlankNodes().put(o.asResource().asNode(), blanknodes.size()-Integer.MIN_VALUE);
                }
            }
        } else if (o.isLiteral()) {
            cc = o.asLiteral().getDatatype().getJavaClass();
        } else {
            Resource r = o.asResource();
            throw new Error(r.isResource()+" Don't know how to deal with "+o.toString());
        }
        if (cc.equals(ResourceImpl.class)) {
            byPredicate.get(p).set(res, o.asResource());
        } else if (cc.equals(BigInteger.class)) {
            byPredicate.get(p).set(res, o.asLiteral().getLong());
        } else if (cc.equals(Integer.class)) {
            byPredicate.get(p).set(res, o.asLiteral().getInt());
        } else if (cc.equals(Long.class)) {
            byPredicate.get(p).set(res, o.asLiteral().getLong());
        } else if (cc.equals(Float.class)) {
            byPredicate.get(p).set(res, o.asLiteral().getFloat());
        } else if (cc.equals(Double.class)) {
            byPredicate.get(p).set(res, o.asLiteral().getDouble());
        } else if (cc.equals(String.class)) {
            byPredicate.get(p).set(res, o.asLiteral().toString());
        }  else if (cc.equals(XSDDateTime.class)) {
          //  byPredicate.get(p).set(res, o.asLiteral().toString());
        } else {
            throw new Error("Can't handle ["+cc+"]  "+stmt);
        }
    }

    @Override
    public void close() {
        if (idx2id!=null) idx2id.close();
        if (IRI2idx!=null) IRI2idx.close();
        if (id2IRI!=null) id2IRI.close();
        if (id2ng!=null) id2ng.close();
        if (ng2id!=null) ng2id.close();
        String bx = roc.getRDE().toString()+"void";
        Resource voidmeta = roc.getRDE().getModel().createResource(bx);
        VoID.generateVoid(voidmeta);
        voidmeta.addProperty(VOID.subset, target);
        roc.getRDE().addProperty(SchemaDO.hasPart, voidmeta);
        writers.forEach((k,b)->{
            roc.Add(target, base, b.getBaseName(), CompressionMethod.STORE, true);
            try (b) {}
            roc.getROCrateWriter().Add(base+"/"+b.getBaseName(), b.getBuffer(), CompressionMethod.STORE);
        });
        byPredicate.forEach((k,paw)->{
            try (paw) {
                
            } catch (OutOfMemoryException ex) {
                logger.error("OVERWATCH : "+k+" "+ex.getMessage());
            } catch (IllegalStateException ex) {
                logger.error("OVERWATCH : "+k+" "+ex.getMessage());
            }
        });
        roc.build();
        try (nt) {}
        try (allocator) {
        } catch (OutOfMemoryException ex) {
            logger.error("Allocator OVERWATCH : "+ex.getMessage());
        }
        vectors.forEach(v->{            
            v.close();
        });
    }
        
    class PredicateProcessor implements Runnable {
        private final PAW pa;
        private final BeakWriter bw;
        
        public PredicateProcessor(PAW pa, BeakWriter bw) {
            this.pa = pa;
            this.bw = bw;
        }

        @Override
        public void run() {
            pa.Finish(bw);
        }
    }
}
