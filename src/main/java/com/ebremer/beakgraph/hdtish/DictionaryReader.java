package com.ebremer.beakgraph.hdtish;

import java.io.File;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public class DictionaryReader { //implements Dictionary {
    
 //   private final BitPackedReader offsets;
  //  private final BitPackedReader integers;
  //  private final BitPackedReader longs;
 //   private final BitPackedWriter datatype;
 //   private DataInputBuffer floats;
 //   private DataInputBuffer doubles;
 //   private FCDWriter text;   
    
    private DictionaryReader(Builder builder) {
        
    }
    
    public static class Builder {
        private File base;
        
        public Builder setBase(File base) {
            this.base = base;
            return this;
        }
        
        public File getBase() {
            return base;
        }
        
        public DictionaryReader build() {
            return new DictionaryReader(this);
        }
    }
    
}
