package com.ebremer.beakgraph.control;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author erich
 */
public class VectorRequestEngine implements Closeable {
    private static volatile VectorRequestEngine instance;
    private final ExecutorService executor;
    
    private VectorRequestEngine() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }
    
    public ExecutorService getExecutorService() {
        return executor;
    }
    
    public static VectorRequestEngine getInstance() {
        if (instance == null) {
            instance = new VectorRequestEngine();
        }
        return instance;
    }

    @Override
    public void close() throws IOException {
        executor.close();
    }
}
