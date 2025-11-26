package com.ebremer.halcyon.geometry;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonGeneratorFactory;
import java.awt.Polygon;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author erich
 */
public class GeoUtil {

    public static String prettyPrint(JsonObject obj) {
        StringWriter sw = new StringWriter();
        try {
            Map<String, Object> properties = new HashMap<>(1);
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
            JsonGeneratorFactory jf = Json.createGeneratorFactory(properties);
            JsonGenerator jg = jf.createGenerator(sw);
            jg.write(obj).close();
        } catch (Exception e) {
        }
        String prettyPrinted = sw.toString();
        return prettyPrinted;
    }
    
    public static String Poly2Json(Polygon p) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonArrayBuilder X = Json.createArrayBuilder();
        JsonArrayBuilder Y = Json.createArrayBuilder();
        for (int i=0; i< p.npoints; i++) {
            X.add(p.xpoints[i]);
            Y.add(p.ypoints[i]);
        }
        job.add("x", X);
        job.add("y", Y);

        return prettyPrint(job.build());
    }
    
    public static void main(String[] args) {
        int x[] = {  10,  30, 40, 50, 110, 140 }; 
        int y[] = { 140, 110, 50, 40,  30,  10 };
        Polygon p = new Polygon(x, y, x.length); 
        System.out.println(Poly2Json(p));
    }   
}