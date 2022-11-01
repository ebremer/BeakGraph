package com.ebremer.beakgraph.arrow;

import com.ebremer.beakgraph.rdf.DataType;
import com.ebremer.beakgraph.rdf.NodeTable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author erbre
 */
public class Dual {
    private int count = 0;
    private IntVector s;
    private ValueVector o;
    private final String p;
    private final DataType c;
    private final String uuid = UUID.randomUUID().toString();
    private final HashMap<String,Pair> idx = new HashMap<>();
    
    public Dual(BufferAllocator allocator, String p, DataType c) {
        this.p = p;
        this.c = c;
        Map<String,String> metadata = new HashMap<>();
        metadata.put("Version", "1.0.0");
        s = new IntVector("SOI"+p, new FieldType(false, Types.MinorType.INT.getType(), new DictionaryEncoding(0, true, new ArrowType.Int(32,true)), metadata), allocator);
        switch (c) {
            case LONG:
                o = new BigIntVector("OOL"+p, allocator);
                break;
            case INTEGER:
                o = new IntVector("OOI"+p, allocator);
                break;
            case FLOAT:
                o = new Float4Vector("OOF"+p, allocator);
                break;
            case STRING:
                o = new VarCharVector("OOS"+p, allocator);
                break;
            case RESOURCE:
                o = new IntVector("OOR"+p, new FieldType(false, Types.MinorType.INT.getType(), new DictionaryEncoding(0, true, new ArrowType.Int(32,true)), metadata), allocator);
                break;
            default:
                throw new Error("Can't handle "+c.toString());
        }
        idx.put("SO",new Pair("SO",s,o));
    }
    
    public String getPredicate() {
        return p;
    }
    
    public String getUUID() {
        return uuid;
    }
    
    public IntVector getSubject() {
        return s;
    }
    
    public ValueVector getObject() {
        return o;
    }

    public void setObject(ValueVector v) {
        if ("http://www.w3.org/2003/12/exif/ns#width".equals(p)) {
            System.out.println(count+" INITX : "+v.getValueCount());
        }
        this.o = v;
    }
    
    public void setSubject(IntVector v) {
        this.s = v;
    }

    public void Finish(Dictionary dictionary){
        System.out.println("Finishing : "+p+"   "+count);
        s.setValueCount(count);
        o.setValueCount(count);
        System.out.println("FINAL  : "+p+"   "+o.getValueCount());
     //   isubject = (IntVector) DictionaryEncoder.encode(s, dictionary);
    }
    
    @Override
    public String toString() {
        int sc = (s==null)?0:s.getValueCount();
        int oc = (o==null)?0:o.getValueCount();
        return "DUAL ======== "+s+" ========= "+o+" =================== "+uuid+" "+sc+"  "+oc;
    }
    
    public void set(int subject, long object) {
        BigIntVector x = (BigIntVector) o;
        x.setSafe(count, object);
        s.setSafe(count, subject);
        count++;
    }

    public void set(int subject, int object) {
        IntVector x = (IntVector) o;
        x.setSafe(count, object);
        s.setSafe(count, subject);
        count++;
    }
    
    public void set(int subject, float object) {
        Float4Vector x = (Float4Vector) o;
        x.setSafe(count, object);
        s.setSafe(count, subject);
        count++;
    }

    public void set(int subject, String object) {
        VarCharVector x = (VarCharVector) o;
        x.setSafe(count, object.getBytes());
        s.setSafe(count, subject);
        count++;
    }
    
    public void set(int subject, Resource object, NodeTable nt) {
        IntVector x = (IntVector) o;
        String os;
        if (object.isAnon()) {
            os = "_:"+object.toString();
        } else {
            os = object.toString();
        }
        //System.out.println("setting "+object);
        x.setSafe(count, nt.getID(os));
        s.setSafe(count, subject);
        count++;
    }
}
