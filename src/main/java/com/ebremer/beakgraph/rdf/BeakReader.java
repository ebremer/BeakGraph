package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.solver.BindingNodeId;
import com.ebremer.beakgraph.solver.BeakIterator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.expr.ExprList;

/**
 *
 * @author erich
 */
public final class BeakReader {
    private NodeTable nodeTable;
    private final HashMap<String,PAR> byPredicate;
    private final BufferAllocator root;
    private Dictionary dictionary;
    
    public BeakReader(File file) throws FileNotFoundException, IOException {
        byPredicate = new HashMap<>();
        root = new RootAllocator();
        Files.list(file.toPath()).forEach(f->{
            FieldVector vx = ReadVector(f);
            if ("dictionary".equals(f.toFile().getName())) {
                DictionaryEncoding dictionaryEncoding = new DictionaryEncoding(0, true, new ArrowType.Int(32, true));
                dictionary = new Dictionary(vx, dictionaryEncoding);
                nodeTable = new NodeTable(dictionary);
            } else {
                StructVector v = (StructVector) vx;
                String p = v.getName();                
                String dt = p.substring(0, 1);
                p = p.substring(1);
                if (!byPredicate.containsKey(p)) {
                    byPredicate.put(p, new PAR(p));
                }
                PAR par = byPredicate.get(p);
                par.put(dt, v);
            }
        });
        //DisplayAll();
    }

    public NodeTable getNodeTable() {
        return nodeTable;
    }
    
    public FieldVector ReadVector(Path p) {
        try {
            FileInputStream fileInputStream = new FileInputStream(p.toFile());
            ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), root);
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
            reader.loadRecordBatch(arrowBlock);
            return vectorSchemaRoot.getVector(0);
        } catch (IOException ex) {
            Logger.getLogger(BeakReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    public void DisplayAll() {
        System.out.println("byPredicate =========================================== "+byPredicate.size());
        byPredicate.forEach((p,par)->{
            par.getAllTypes().forEach((dt,s)->{
                System.out.println(p+" "+dt+" "+s.getValueCount());
            });
        });
        System.out.println("byPredicate ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    }

    public Iterator<BindingNodeId> Read(BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        //System.out.println(" ______________________________________TRIPLE : "+triple+"   FILTER : "+filter);
        //System.out.println(triple.getSubject().toString());
        //Node s = triple.getSubject();
        //boolean blank = s.isBlank();
        //System.out.println(blank);
        //System.out.println(triple.getSubject().isVariable()+" "+triple.getObject().isVariable());
        if (byPredicate.containsKey(triple.getPredicate().getURI())) {
            PAR par = byPredicate.get(triple.getPredicate().getURI());
            LinkedList<Iterator<BindingNodeId>> its = new LinkedList<>();
            par.getAllTypes().forEach((k,dual)->{
                Iterator<BindingNodeId> i = new BeakIterator(bnid, k, dual, triple, filter, nodeTable);
                its.add(i);
            });
            return new IteratorChain(its);
        }
        return new IteratorChain(new LinkedList<>());
    }
}