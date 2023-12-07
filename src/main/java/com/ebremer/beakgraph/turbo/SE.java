package com.ebremer.beakgraph.turbo;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class SE extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Random rand = new Random();
        try {
            Thread.sleep(rand.nextInt(10)+1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(SE.class.getName()).log(Level.SEVERE, null, ex);
        }
        request.getHeaderNames().asIterator().forEachRemaining(h->{
        //System.out.println(h);
                });
        System.out.println(request.getQueryString());
        //System.out.println(request.getHeader("Accept"));
        response.setContentType("application/sparql-results+json");
        //response.getWriter().println("<h1>Hello World GET</h1>");
        Model m = ModelFactory.createDefaultModel();
        m.createResource().addLiteral(RDF.value, rand.nextFloat());
        String sparql = request.getParameter("query");
        //System.out.println("SPARQL --> "+sparql);
        QueryExecution qe = QueryExecutionFactory.create(sparql,m);
        ResultSet rs = qe.execSelect();
        OutputStream sos = response.getOutputStream();
        ResultSetFormatter.outputAsJSON(sos, rs);
    }
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        response.getWriter().println("<h1>Hello World POST</h1>");
    }
    
}
