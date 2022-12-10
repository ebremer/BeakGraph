package com.ebremer.beakgraph.rdf;

import static com.ebremer.beakgraph.rdf.DataType.FLOAT;
import static com.ebremer.beakgraph.rdf.DataType.INTEGER;
import static com.ebremer.beakgraph.rdf.DataType.LONG;
import static com.ebremer.beakgraph.rdf.DataType.RESOURCE;
import static com.ebremer.beakgraph.rdf.DataType.STRING;
import static com.ebremer.beakgraph.rdf.DualSort.ColumnOrder.OS;
import static com.ebremer.beakgraph.rdf.DualSort.ColumnOrder.SO;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author erich
 */
public class PAW {
    private final HashMap<DataType,StructVector> cs = new HashMap<>();
    private final HashMap<DataType,Integer> counts = new HashMap<>();
    private final BufferAllocator allocator;
    private final NodeTable nt;
    private final String p;
    
    public PAW(BufferAllocator allocator, NodeTable nt, String p) {
        this.allocator = allocator;
        this.nt = nt;
        this.p = p;
    }
    
    public String getPredicate() {
        return p;
    }
    
    public void Count(DataType datatype) {
        if (!counts.containsKey(datatype)) {
            counts.put(datatype, 0);
        }
        int c = counts.remove(datatype);
        c++;
        counts.put(datatype, c);
    }
    
    /*
    private StructVector cloneVector(StructVector vector) {
        final FieldType fieldType = vector.getField().getFieldType();
        StructVector cloned = (StructVector) fieldType.createNewSingleVector(vector.getField().getName(), allocator, null);
        final ArrowFieldNode fieldNode = new ArrowFieldNode(vector.getValueCount(), vector.getNullCount());
        cloned.loadFieldBuffers(fieldNode, vector.getFieldBuffers());
        cloned.setValueCount(vector.getValueCount());
        return cloned;
    }*/
    
    public void buildblank(StructVector src, StructVector destVector) {
        IntVector subject = destVector.addOrGet("s", src.getChild("s").getField().getFieldType(), IntVector.class);
        subject.allocateNew(src.valueCount);
        switch (src.getChild("o")) {
            case IntVector o -> {
                IntVector object = destVector.addOrGet("o", o.getField().getFieldType(), IntVector.class);
                object.allocateNew(src.valueCount);
            }
            case BigIntVector o -> {
                BigIntVector object = destVector.addOrGet("o", o.getField().getFieldType(), BigIntVector.class);
                object.allocateNew(src.valueCount);
            }
            case Float4Vector o -> {
                Float4Vector object = destVector.addOrGet("o", o.getField().getFieldType(), Float4Vector.class);
                object.allocateNew(src.valueCount);
            }
            case VarCharVector o -> {
                VarCharVector object = destVector.addOrGet("o", o.getField().getFieldType(), VarCharVector.class);
                object.allocateNew(src.valueCount);
            }
            case default -> throw new Error("can't handle this");
        }
        destVector.setValueCount(src.valueCount);
    }
    
    public class HalfSort implements Runnable {
        private final StructVector src;
        private final StructVector dest;
        private final Semaphore semaphore;
        
        public HalfSort(Semaphore semaphore, StructVector src, StructVector dest) {
            this.semaphore = semaphore;
            this.src = src;
            this.dest = dest;
        }

        @Override
        public void run() {
            semaphore.tryAcquire();
            src.getChildFieldNames().forEach(c->{
            System.out.println("CHILD : "+c);
            });
            System.out.println((src.getChild("s")==null)+" VECTOR IS : "+src);
            DualSort dualSort = new DualSort();
            dualSort.Sort(src, dest, SO);
            semaphore.release();
        }
    }
    
    public StructVector upgrade(DataType datatype, StructVector src, Job job) {
        String type;
        switch (datatype) {
            case INTEGER: type = "I"; break;
            case LONG: type = "L"; break;
            case FLOAT: type = "F"; break;
            case STRING: type = "S"; break;
            case RESOURCE: type = "R"; break;
            default: type = "X"; break;
        }
        StructVector top = StructVector.emptyWithDuplicates(type+p, src.getAllocator());
        top.setValueCount(src.valueCount);
        StructVector so = top.addOrGet("so", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        StructVector os = top.addOrGet("os", new FieldType(false, Types.MinorType.STRUCT.getType(), null, null), StructVector.class);
        job.status = "BUILD BLANK";
        buildblank(src, so);
        buildblank(src, os);
        //Semaphore semaphore = new Semaphore(2);
        //DualSort ds = new DualSort();
        //System.out.println("LAUNCH ZECTOR IS : "+src);
        System.out.println("Vector : "+p+" has length "+src.getValueCount());
        /*
        if ("https://www.ebremer.com/halcyon/ns/hasRange/1".equals(p)) {
            IntStream.range(0, src.valueCount).forEach(i->{
                IntVector s = (IntVector) src.getChild("s");
                IntVector o = (IntVector) src.getChild("o");
                System.out.println(i+" : "+s.get(i)+" --> "+o.get(i));
            });
        }*/
        DualSort dualSort = new DualSort();
        job.status = "SORT 1";
        dualSort.Sort(src, so, SO);
        job.status = "SORT 2";
        dualSort.Sort(src, os, OS);
        job.status = "SET INDEX";
        IntStream.range(0, src.getValueCount()).forEach(r->{
            top.setIndexDefined(r);
        });
        
        //new Thread(new HalfSort(semaphore,src,so)).start();
        //new Thread(new HalfSort(semaphore,src,os)).start();
        //while (semaphore.availablePermits()!=2) {}
        //src.close();
        /*
        System.out.println("UPGRADE :=======================================\n"+
                src+"\n-----------------------------------\n"+
                so+"\n-----------------------------------\n"+
                os+"\n--------------- SO --------------------\n"+
                top.getChild("so")+"\n-------------- OS --------------------\n"+
                top.getChild("os")+"\n------------- TOP --------------------\n"+
                top+"\n"+top.getValueCount()+" ================================================");
        */
        return top;
    }
    
    public void DisplayVector(ValueVector v) {
        switch (v) {
            case IntVector x -> {System.out.println("IntVector ["+x.getValueCount()+"]: "+x);}
            case BigIntVector x -> {System.out.println("BigIntVector ["+x.getValueCount()+"]: "+x);}
            case VarCharVector x -> {System.out.println("VarCharVector ["+x.getValueCount()+"]: "+x);}
            case Float4Vector x -> {System.out.println("Float4Vector ["+x.getValueCount()+"]: "+x);}
            default -> {throw new Error("can't handle "+v.getClass().toGenericString());}
        }
    }
    
    public void Finish(CopyOnWriteArrayList<Field> fields, CopyOnWriteArrayList<FieldVector> vectors, Job job) {
        //System.out.println("Finishing : "+p+" "+cs.size());
        cs.forEach((k,v)->{
            v.getWriter().setValueCount(counts.get(k));
            //System.out.println(p+" ["+k+"] AAA >>> "+counts.get(k)+ " XYXYXYXY "+v.getValueCount()+" === this finish ---> "+v.getChild("o").getValueCount());
            //System.out.println(p+" ZAM ===> "+v);
            //DisplayVector(v.getChild("s"));
            //DisplayVector(v.getChild("o"));
            StructVector z = upgrade(k,v,job);
            //System.out.println(p+" ["+k+"] XXX >>> "+z.getValueCount()+"  "+z);
            //System.out.println("BF/V : "+fields.size()+" "+vectors.size());
            fields.add(z.getField());
            vectors.add(z);
            //System.out.println("AF/V : "+fields.size()+" "+vectors.size());
        });
    }
    
    public StructVector build(DataType datatype) {
        Map<String, String> smeta = new HashMap<>();
        smeta.put("rdf:type","rdfs:Resource");
        smeta.put("rdfs:range", "xs:int");
        Map<String, String> ometa = new HashMap<>();
        FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        StructVector ss = new StructVector("STRUCTME", allocator, fieldType, null, AbstractStructVector.ConflictPolicy.CONFLICT_APPEND, true);
        //ss.addOrGet("s", new FieldType(false, Types.MinorType.INT.getType(), nt.getDictionary().getEncoding(), smeta), IntVector.class);  commented out to break out dictionary.
        ss.addOrGet("s", new FieldType(false, Types.MinorType.INT.getType(), null, smeta), IntVector.class);
        switch (datatype) {
            case INTEGER:
                ometa.put("rdf:type","rdfs:Literal");
                ometa.put("rdfs:range", "xs:int");
                ss.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), null, ometa), IntVector.class);
                break;
            case LONG:
                ometa.put("rdf:type","rdfs:Literal");
                ometa.put("rdfs:range", "xs:long");
                ss.addOrGet("o", new FieldType(false, Types.MinorType.BIGINT.getType(), null, ometa), BigIntVector.class);
                break;
            case FLOAT:
                ometa.put("rdf:type","rdfs:Literal");
                ometa.put("rdfs:range", "xs:float");
                ss.addOrGet("o", new FieldType(false, Types.MinorType.FLOAT4.getType(), null, ometa), Float4Vector.class);
                break;
            case STRING:
                ometa.put("rdf:type","rdfs:Literal");
                ometa.put("rdfs:range", "xs:string");
                ss.addOrGet("o", new FieldType(false, Types.MinorType.VARCHAR.getType(), null, ometa), VarCharVector.class);
                break;
            case RESOURCE:
                ometa.put("rdf:type","rdfs:Resource");
                ometa.put("rdfs:range", "rdfs:Resource");
                //ss.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), nt.getDictionary().getEncoding(), ometa), IntVector.class);
                ss.addOrGet("o", new FieldType(false, Types.MinorType.INT.getType(), null, ometa), IntVector.class);
                break;
        }
        return ss;
    }
    
    public void set(String s, int o) {
        StructVector sv;
        if (!cs.containsKey(INTEGER)) {
            cs.put(INTEGER, build(INTEGER));
        }
        sv = cs.get(INTEGER);
        Count(INTEGER);
        NullableStructWriter writer = sv.getWriter();
        writer.start();
        writer.integer("s").writeInt(nt.getID(s));
        writer.integer("o").writeInt(o);
        writer.end();
    }
    
    public void set(String s, long o) {
        StructVector sv;
        if (!cs.containsKey(LONG)) {
            cs.put(LONG, build(LONG));
        }
        sv = cs.get(LONG);
        Count(LONG);
        NullableStructWriter writer = sv.getWriter();
        writer.start();
        writer.integer("s").writeInt(nt.getID(s));
        writer.bigInt("o").writeBigInt(o);
        writer.end();
    }
    
    public void set(String s, float o) {
        StructVector sv;
        if (!cs.containsKey(FLOAT)) {
            cs.put(FLOAT, build(FLOAT));
        }
        sv = cs.get(FLOAT);
        Count(FLOAT);
        NullableStructWriter writer = sv.getWriter();
        writer.start();
        writer.integer("s").writeInt(nt.getID(s));
        writer.float4("o").writeFloat4(o);
        writer.end();
    }
    
    public void set(String s, String o) {
        StructVector sv;
        if (!cs.containsKey(STRING)) {
            cs.put(STRING, build(STRING));
        }
        sv = cs.get(STRING);
        Count(STRING);
        NullableStructWriter writer = sv.getWriter();
        writer.start();
        writer.integer("s").writeInt(nt.getID(s));
        byte[] bytes = o.getBytes();
        ArrowBuf tempBuf = allocator.buffer(bytes.length);
        tempBuf.setBytes(0, bytes);
        writer.varChar("o").writeVarChar(0, bytes.length, tempBuf);
        writer.end();
    }
    
    public void set(String s, Resource object) {
        StructVector sv;
        //System.out.println("set : "+object.getClass().toGenericString());
        if (!cs.containsKey(RESOURCE)) {
            cs.put(RESOURCE, build(RESOURCE));
        }
        sv = cs.get(RESOURCE);
        Count(RESOURCE);
        String os;
        if (object.isAnon()) {
            os = "_:"+object.toString();
        } else {
            os = object.toString();
        }
        NullableStructWriter writer = sv.getWriter();
        writer.start();
        writer.integer("s").writeInt(nt.getID(s));
        writer.integer("o").writeInt(nt.getID(os));
        writer.end();
    }
}
