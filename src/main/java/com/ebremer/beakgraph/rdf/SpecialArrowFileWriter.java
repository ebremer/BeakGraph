package com.ebremer.beakgraph.rdf;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

/**
 *
 * @author erich
 */
public class SpecialArrowFileWriter extends ArrowFileWriter {
    
    public SpecialArrowFileWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
        super(root, provider, out);
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
