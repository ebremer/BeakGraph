package com.ebremer.beakgraph.ng;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author erich
 */
public class Mapper {
    //private final URI file;
    //private final URI http;
    private URI nfile;
    //private final URI nhttp;
    private final int httpbaselength;
    
    public Mapper(URI file, URI http) {
        try {
            //this.file = file;
            //this.http = http;
            this.nfile = new URI(null, null, file.getPath(), null);
        } catch (URISyntaxException ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        //this.nhttp = new URI(null, null, http.getPath(), null);
        httpbaselength = http.getPath().length();
    }

    public String Base2Src(URI frag) {
        String sfrag =  frag.getPath().substring(httpbaselength);
        URI nuri;
        try {
            nuri = new URI(null, null, frag.getPath(), null);
            nuri = nuri.relativize(nfile);
            return nuri.getPath()+sfrag;
        } catch (URISyntaxException ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static void main(String args[]) throws URISyntaxException {
        URI asrc = new URI("file:///a/b/d/e/HalcyonStorage/tcga/brca/2024/image.zip");
        URI abase = new URI(      "https://ebremer.com/POD/tcga/brca/2024/image.zip");
        Mapper mapper = new Mapper(asrc,abase);
        System.out.println(mapper.Base2Src(new URI("https://ebremer.com/POD/tcga/brca/2024/image.zip/sub/a/b/c/array.arrow")));
    }
}
