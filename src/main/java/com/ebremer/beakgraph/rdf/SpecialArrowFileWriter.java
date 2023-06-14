package com.ebremer.beakgraph.rdf;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;

/**
 *
 * @author erich
 */
public class SpecialArrowFileWriter extends ArrowFileWriter {
    
    public SpecialArrowFileWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out, Map<String, String> metaData) {
        super(root, provider, out, metaData);
    }
    
    public SpecialArrowFileWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out,
                            Map<String, String> metaData, IpcOption option, CompressionCodec.Factory compressionFactory,
                             CompressionUtil.CodecType codecType) {
    super(root, provider, out, metaData, option, compressionFactory, codecType);
  }
    
    @Override
    public void close() {
        try {
            end();
        } catch (IOException ex) {
            Logger.getLogger(SpecialArrowFileWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
