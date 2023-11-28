package com.ebremer.beakgraph.ng;

import java.util.stream.IntStream;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;

/**
 *
 * @author erich
 */
public class Tools {
    
    public static void DisplayDual(StructVector sv) {
        StructVector so = (StructVector) sv.getChild("so");
        StructVector os = (StructVector) sv.getChild("os");
        IntVector sos = (IntVector) so.getChild("s");
        IntVector soo = (IntVector) so.getChild("o");
        IntVector oss = (IntVector) os.getChild("s");
        IntVector oso = (IntVector) os.getChild("o");
        IntStream.range(0, sos.getValueCount()).forEach(i->{
            System.out.println(sos.get(i)+" "+soo.get(i)+" ===== "+oss.get(i)+" "+oso.get(i));
        });
    }    
}
