package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.Params;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.sparql.core.DatasetGraph;

/**
 *
 * @author erich
 */
public class Raptor extends HttpServlet {
    private final String hostname;
    
    public Raptor() {
        System.out.println("Starting Raptor Server...");
        hostname = "http://localhost";
    }
    
    @Override
    protected void doGet( HttpServletRequest request, HttpServletResponse response ) {
        String hh = hostname+"/"+request.getRequestURI().substring("/raptor/".length());
        /*
        URI uri = HURI.of(hh);
        BeakGraph bg = BeakGraphPool.getPool().borrowObject(uri);
        DatasetGraph dsg = new BGDatasetGraph(bg);
        Dataset ds = DatasetFactory.wrap(dsg);
        Query query = QueryFactory.create(request.getParameter("query"));
        if (query.isSelectType()) {
            response.setContentType("application/sparql-results+json");
            QueryExecution qe = QueryExecutionFactory.create(query,ds);
            ResultSet rs = qe.execSelect();
            try (ServletOutputStream sos = response.getOutputStream()) {
                ResultSetFormatter.outputAsJSON(sos, rs);
            } catch (IOException ex) {
                Logger.getLogger(Raptor.class.getName()).log(Level.SEVERE, null, ex);
            }
            BeakGraphPool.getPool().returnObject(uri, bg);
        } else {
            INFO(request,response);
        }*/
        INFO(request,response);
    }

    @Override
    protected void doPost( HttpServletRequest request, HttpServletResponse response ) {
        INFO(request,response);
    }
    
    private void INFO(HttpServletRequest request, HttpServletResponse response) {
        response.setContentType("text/plain");
        String wow = request.toString()+"\n"+
                request.getRequestURI()+"\n"+request.getProtocol()+"\n"+request.getMethod()+"\n"+request.getQueryString();
        System.out.println(wow);
        try (PrintWriter writer=response.getWriter()) {
            writer.append(wow);
        } catch (IOException ex) {
            Logger.getLogger(Raptor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
