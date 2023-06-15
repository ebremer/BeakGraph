package com.ebremer.beakgraph.ng;

import static com.ebremer.beakgraph.ng.DataType.DOUBLE;
import static com.ebremer.beakgraph.ng.DataType.FLOAT;
import static com.ebremer.beakgraph.ng.DataType.INTEGER;
import static com.ebremer.beakgraph.ng.DataType.LONG;
import static com.ebremer.beakgraph.ng.DataType.RESOURCE;
import static com.ebremer.beakgraph.ng.DataType.STRING;
import static com.ebremer.beakgraph.ng.DualSort.ColumnOrder.OS;
import static com.ebremer.beakgraph.ng.DualSort.ColumnOrder.SO;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class PAW implements AutoCloseable {
    private final HashMap<DataType,StructVector> cs = new HashMap<>();
    private final HashMap<DataType,Integer> totalcounts = new HashMap<>();
    private final HashMap<DataType,Integer> counts = new HashMap<>();
    private final BufferAllocator allocator;
    private final NodeTable nt;
    private final String p;
    private static final Logger logger = LoggerFactory.getLogger(PAW.class);
    
    public PAW(BufferAllocator allocator, NodeTable nt, String p) {
        this.allocator = allocator;
        this.nt = nt;
        this.p = p;
    }
    
    public void clearTotalCounts() {
        totalcounts.clear();
    }
    
    public int getCounts(DataType dt) {
        if (totalcounts.containsKey(dt)) {
            return totalcounts.get(dt);
        }
        return 0;
    }
    
    public void sumCounts() {
        counts.keySet().forEach(k->{
            if (totalcounts.containsKey(k)) {
                int c = totalcounts.get(k) + counts.get(k);
                totalcounts.put(k, c);
            } else {
                totalcounts.put(k, counts.get(k));
            }
        });
    }
    
    public void resetCounts() {
        counts.keySet().forEach(k->{
            counts.put(k, 0);
        });
    }
    
    public void resetVectors() {
        cs.forEach((k,v)->{
            v.reset();
        });
    }
    
    public void allocateVectors() {
        cs.forEach((k,v)->{
            v.clear();
            v.allocateNew();
        });
    }
    
    @Override
    public void close() {
        cs.forEach((k,v)->{
            try (v) {}
        });
        try (allocator) {
        } catch (OutOfMemoryException ex) {
            logger.error("Inside PAW OutOfMemoryException : "+p+" "+ex.getMessage());
        } catch (IllegalStateException ex) {
            logger.error("Inside PAW IllegalStateException : "+p+" "+ex.getMessage());
        }
        cs.clear();
        counts.clear();
    }
    
    public HashMap<DataType,StructVector> getCS() {
        return cs;
    }
    
    public String getPredicate() {
        return p;
    }
    
    public void Count(DataType datatype) {
        if (!counts.containsKey(datatype)) {
            counts.put(datatype, 0);
        }
        int c = counts.get(datatype)+1;
        counts.put(datatype, c);
    }
    
    public void buildblank(StructVector src, StructVector dest, int count) {
        IntVector subject = dest.addOrGet("s", new FieldType(false, Types.MinorType.INT.getType(), null, src.getChild("s").getField().getMetadata()), IntVector.class);
        subject.allocateNew(count);
        Object aa = src.getChild("o");
        if (aa instanceof IntVector o) {
             IntVector object = dest.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), null, o.getField().getMetadata()), IntVector.class);
            object.allocateNew(count);            
        } else if (aa instanceof BigIntVector o) {
            BigIntVector object = dest.addOrGet("o", new FieldType(false, Types.MinorType.BIGINT.getType(), null, o.getField().getMetadata()), BigIntVector.class);
            object.allocateNew(count);          
        } else if (aa instanceof Float4Vector o) {
            Float4Vector object = dest.addOrGet("o", new FieldType(false, Types.MinorType.FLOAT4.getType(), null, o.getField().getMetadata()), Float4Vector.class);
            object.allocateNew(count);  
        } else if (aa instanceof Float8Vector o) {
            Float8Vector object = dest.addOrGet("o", new FieldType(false, Types.MinorType.FLOAT8.getType(), null, o.getField().getMetadata()), Float8Vector.class);
            object.allocateNew(count);  
        } else if (aa instanceof VarCharVector o) {
            VarCharVector object = dest.addOrGet("o", new FieldType(false, Types.MinorType.VARCHAR.getType(), null, o.getField().getMetadata()), VarCharVector.class);
            object.allocateNew(count);
        } else {
            throw new Error("can't handle this");
        }
        dest.setValueCount(count);
    }
    
    private StructVector upgrade(DataType datatype, StructVector src, StructVector dest, int count) {
        StopWatch sw = StopWatch.getInstance();
        StructVector so;
        StructVector os;
        if (dest==null) {
            dest = new StructVector(p, src.getAllocator(), FieldType.notNullable(ArrowType.Struct.INSTANCE), null, AbstractStructVector.ConflictPolicy.CONFLICT_REPLACE, true);
            dest.setValueCount(src.getValueCount());
            so = dest.addOrGet("so", FieldType.notNullable(Types.MinorType.STRUCT.getType()), StructVector.class);
            os = dest.addOrGet("os", FieldType.notNullable(Types.MinorType.STRUCT.getType()), StructVector.class);
            buildblank(src, so, count);
            buildblank(src, os, count);
        } else {
            dest.reset();
            dest.setValueCount(src.getValueCount());
            so = (StructVector) dest.getChild("so");
            os = (StructVector) dest.getChild("os");
        }
        logger.trace(sw.Lapse("Upgraded Vector : "+p+" "+datatype+"  "+src.valueCount));
        sw.reset();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            logger.trace(sw.Lapse("Submit Sort : "+p+" "+datatype+" SO"));
            executor.submit(() -> {
                DualSort dualSort = new DualSort();
                dualSort.Sort(src, so, SO);
                logger.trace(sw.Lapse("Sorted : "+p+" "+datatype+" SO"));
            });
            logger.trace(sw.Lapse("Submit Sort : "+p+" "+datatype+" OS"));
            executor.submit(() -> {
                DualSort dualSort = new DualSort();
                dualSort.Sort(src, os, OS);
                logger.trace(sw.Lapse("Sorted : "+p+" "+datatype+" OS"));
            });
        }
//        if (src.getValueCount()>1000000) {
          // if ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p)) {
            //System.out.println("CRACK =================> "+p+"  "+datatype+"  "+dest.getValueCount());
            //Tools.DisplayDual(dest);
        //}
        logger.trace(sw.Lapse("trace : "+p+" "+datatype));
        return dest;
    }
    
    public void Finish(BeakWriter bw) {
        cs.forEach((k,v)->{
            int count = counts.get(k);
            v.setValueCount(count);
            for (int z=0; z<count; z++) {
                v.setIndexDefined(z);
            }
            v.setValueCount(count);
            if (counts.get(k)>0) {
                Writer block = bw.getWriter(v.getName());
                if (!block.isInitialized()) {
                    StructVector zz = upgrade(k,v,null,counts.get(k));
                    block.setVector(zz, this);
                } else {
                    upgrade(k,v,(StructVector) block.getVector(),counts.get(k));
                }
            }
        });
    }
    
    private StructVector build(String name, DataType datatype) {
        Map<String, String> smeta = new HashMap<>();
        smeta.put(RDF.type.getURI(),RDFS.Resource.getURI());
        smeta.put(RDFS.range.getURI(), XSD.xint.getURI());
        Map<String, String> ometa = new HashMap<>();
        FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        StructVector ss = new StructVector(name, allocator, fieldType, null, AbstractStructVector.ConflictPolicy.CONFLICT_REPLACE, false);
        ss.addOrGet("s", new FieldType(false, Types.MinorType.INT.getType(), null, smeta), IntVector.class);
        switch (datatype) {
            case INTEGER:
                ometa.put(RDF.type.getURI(),RDFS.Literal.getURI());
                ometa.put(RDFS.range.getURI(), XSD.xint.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), null, ometa), IntVector.class);
                break;
            case LONG:
                ometa.put(RDF.type.getURI(),RDFS.Literal.getURI());
                ometa.put(RDFS.range.getURI(),XSD.xlong.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.BIGINT.getType(), null, ometa), BigIntVector.class);
                break;
            case FLOAT:
                ometa.put(RDF.type.getURI(),RDFS.Literal.getURI());
                ometa.put(RDFS.range.getURI(), XSD.xfloat.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.FLOAT4.getType(), null, ometa), Float4Vector.class);
                break;
            case DOUBLE:
                ometa.put(RDF.type.getURI(),RDFS.Literal.getURI());
                ometa.put(RDFS.range.getURI(), XSD.xdouble.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.FLOAT8.getType(), null, ometa), Float8Vector.class);
                break;
            case STRING:
                ometa.put(RDF.type.getURI(),RDFS.Literal.getURI());
                ometa.put(RDFS.range.getURI(), XSD.xstring.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.VARCHAR.getType(), null, ometa), VarCharVector.class);
                break;
            case RESOURCE:
                ometa.put(RDF.type.getURI(),RDFS.Resource.getURI());
                ometa.put(RDFS.range.getURI(), XSD.anyURI.getURI());
                ss.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), null, ometa), IntVector.class);
                break;
        }
        return ss;
    }
    
    public void Handle(Resource dt) {
        if (dt==null) {
            if (!cs.containsKey(RESOURCE)) {
                cs.put(RESOURCE, build(p,RESOURCE));
            }
        } else if (dt.equals(XSD.xstring)) {
            if (!cs.containsKey(STRING)) {
                cs.put(STRING, build(p,STRING));
            }
        } else if (dt.equals(XSD.xlong)) {
            if (!cs.containsKey(LONG)) {
                cs.put(LONG, build(p,LONG));
            }
        } else if (dt.equals(XSD.xfloat)) {
            if (!cs.containsKey(FLOAT)) {
                cs.put(FLOAT, build(p,FLOAT));
            }
        } else if (dt.equals(XSD.xdouble)) {
            if (!cs.containsKey(DOUBLE)) {
                cs.put(DOUBLE, build(p,DOUBLE));
            }    
        } else if (dt.equals(XSD.xint)) {
            if (!cs.containsKey(INTEGER)) {
                cs.put(INTEGER, build(p,INTEGER));
            }
        } else if (dt.equals(XSD.dateTime)) {
         //   if (!cs.containsKey(INTEGER)) {
           //     cs.put(INTEGER, build("I"+p,INTEGER));
            //}
        } else if (dt.equals(XSD.integer)) {
            if (!cs.containsKey(LONG)) {
                cs.put(LONG, build(p,LONG));
            }    
        } else {
            throw new Error("WHAT IS THIS "+dt);
        }
    }
    
    public void set(Resource s, int o) {
        int index = counts.containsKey(INTEGER)?counts.get(INTEGER):0;
        StructVector sv = cs.get(INTEGER);
        IntVector ss = (IntVector) sv.getChild("s");
        IntVector oo = (IntVector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, o);
        Count(INTEGER);
    }
    
    public void set(Resource s, long o) {
        try {
        int index = counts.containsKey(LONG)?counts.get(LONG):0;
        StructVector sv = cs.get(LONG);
        IntVector ss = (IntVector) sv.getChild("s");
        BigIntVector oo = (BigIntVector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, o);
        Count(LONG);}
        catch (NullPointerException ex) {
            logger.error("CRUNCH ---> "+s+" "+p+" "+o);
        }
    }
    
    public void set(Resource s, float o) {
        int index = counts.containsKey(FLOAT)?counts.get(FLOAT):0;
        StructVector sv = cs.get(FLOAT);
        IntVector ss = (IntVector) sv.getChild("s");
        Float4Vector oo = (Float4Vector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, o);
        Count(FLOAT);
    }
    
    public void set(Resource s, double o) {
        int index = counts.containsKey(DOUBLE)?counts.get(DOUBLE):0;
        StructVector sv = cs.get(DOUBLE);
        IntVector ss = (IntVector) sv.getChild("s");
        Float8Vector oo = (Float8Vector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, o);
        Count(DOUBLE);
    }
    
    public void set(Resource s, String o) {
        int index = counts.containsKey(STRING)?counts.get(STRING):0;
        StructVector sv = cs.get(STRING);
        IntVector ss = (IntVector) sv.getChild("s");
        VarCharVector oo = (VarCharVector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, new Text(o.getBytes()));
        Count(STRING);
    }
    
    public void set(Resource s, Resource o) {
        int index = counts.containsKey(RESOURCE)?counts.get(RESOURCE):0;
        StructVector sv = cs.get(RESOURCE);
        IntVector ss = (IntVector) sv.getChild("s");
        IntVector oo = (IntVector) sv.getChild("o");
        ss.setSafe(index, nt.getID(s));
        oo.setSafe(index, nt.getID(o));
        Count(RESOURCE);
    }
}
