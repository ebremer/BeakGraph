package com.ebremer.beakgraph.cmdline;

import java.util.concurrent.atomic.AtomicLong;

public class FileCounter {    
    private final AtomicLong directoryCount = new AtomicLong(0);
    private final AtomicLong rdfFileCount = new AtomicLong(0);
    private final AtomicLong otherFileCount = new AtomicLong(0);
    
    private final AtomicLong zeroLengthFileCount = new AtomicLong(0);
    private final AtomicLong failedConversionFileCount = new AtomicLong(0); 

    public void incrementOtherFileCount() {
        otherFileCount.incrementAndGet();
    }

    public void incrementDirectoryCount() {
        directoryCount.incrementAndGet();
    }

    public void incrementRDFFileCount() {
        rdfFileCount.incrementAndGet();
    }

    public void incrementZeroLengthFileCount() {
        zeroLengthFileCount.incrementAndGet();
    }
    
    public void incrementFailedConversionFileCount() {
        failedConversionFileCount.incrementAndGet();
    }
    
    public long getOtherFileCount() {
        return otherFileCount.get();
    }

    public long getDirectoryCount() {
        return directoryCount.get();
    }

    public long getRDFFileCount() {
        return rdfFileCount.get();
    }
    
    public long getZeroFileCount() {
        return zeroLengthFileCount.get();
    }
    
    public long getFailedConversionFileCount() {
        return failedConversionFileCount.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("""
            ================================            
            Directories            : %d            
            RDF files              : %d
            Other files            : %d
            ================================
            """,            
            getDirectoryCount(),                        
            getRDFFileCount(),
            getOtherFileCount()
        ));
        sb.append(String.format("""
            Zero Length files      : %d
            Failed Conversions     : %d
            Successful Conversions : %d
            ================================
            """,
            getZeroFileCount(),
            getFailedConversionFileCount(),
            getRDFFileCount()-getZeroFileCount()-getFailedConversionFileCount()
        ));
        return sb.toString();
    }
}
