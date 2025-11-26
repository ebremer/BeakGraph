package com.ebremer.beakgraph.ng;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;

/**
 *
 * @author erich
 */
public class Writer implements AutoCloseable {
    private final String base;
    private FieldVector vector;
    private VectorSchemaRoot root;
    private ArrowFileWriter writer;
    private boolean initialized = false;
    private String basename = null;
    private PAW paw;
    private final ByteArrayOutputStream bos;
    private final Map<String, String> meta = new HashMap<>();
    
    public Writer(String base) {
        this.base = base;
        this.vector = null;
        this.bos = new ByteArrayOutputStream(1024*1024*1024*325);
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    public FieldVector getVector() {
        return vector;
    }
    
    public String getBaseName() {
        return basename;
    }
    
    public VectorSchemaRoot getVectorSchemaRoot() {
        return root;
    }
    
    public ArrowFileWriter getWriter() {
        return writer;
    }
    
    public byte[] getBuffer() {
        return bos.toByteArray();
    }
    
    public long getBufferSize() {
        return bos.toByteArray().length;
    }
    
    public void setVector(FieldVector v, PAW paw) {
        this.vector = v;
        this.paw = paw;
        basename = UUID.randomUUID().toString()+".arrow";
        try {
            meta.put("predicate", v.getName().substring(1,v.getName().length()));
            meta.put("datatype", v.getName().substring(0,1));
            meta.put("triples", String.valueOf(v.getValueCount()));  
            root = new VectorSchemaRoot(List.of(v.getField()), List.of(v));
            writer = new ArrowFileWriter(root, null, Channels.newChannel(bos), meta, IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD);
            writer.start();
            this.initialized = true;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public PAW getPAW() {
        return paw;
    }

    @Override
    public void close() {
        if (vector!=null) vector.close();
        if (root!=null) root.close();
        if (writer!=null) {
            try {
                writer.end();
                writer.close();
            } catch (IOException ex) {
                Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (bos!=null) {
            try (bos) {} catch (IOException ex) {
                Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
