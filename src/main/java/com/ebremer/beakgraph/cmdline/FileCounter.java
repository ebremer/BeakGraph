package com.ebremer.beakgraph.cmdline;

import java.util.concurrent.atomic.AtomicLong;

public class FileCounter {    
    private final AtomicLong directoryCount = new AtomicLong(0);
    private final AtomicLong rdfFileCount = new AtomicLong(0);
    private final AtomicLong otherFileCount = new AtomicLong(0);    
    private final AtomicLong zeroLengthFileCount = new AtomicLong(0);
    private final AtomicLong failedConversionFileCount = new AtomicLong(0); 
    private final AtomicLong skippedExistingCount = new AtomicLong(0);
    private final AtomicLong unreadableCount = new AtomicLong(0);

    /** A directory or file the walk could not read (ACL-denied folder, junction cycle): logged and skipped (BG-418). */
    public void incrementUnreadableCount() {
        unreadableCount.incrementAndGet();
    }

    public long getUnreadableCount() {
        return unreadableCount.get();
    }

    /** A source whose non-empty destination .h5 already existed (per-file mode leaves it alone). */
    public void incrementSkippedExistingCount() {
        skippedExistingCount.incrementAndGet();
    }

    public long getSkippedExistingCount() {
        return skippedExistingCount.get();
    }

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

    /**
     * RDF files that were actually WRITTEN this run: neither failed nor
     * skipped because a non-empty destination already existed (skips have
     * their own row; counting them as successes hid a stale store behind a
     * green summary, BG-157). Zero-length files are NOT subtracted here: the
     * traverse filter rejects them before they are ever counted as RDF files,
     * so subtracting them again understated the summary and could drive it
     * negative.
     */
    public long getSuccessfulConversionCount() {
        return getRDFFileCount() - getFailedConversionFileCount() - getSkippedExistingCount();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(java.util.Locale.ROOT, """
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
        sb.append(String.format(java.util.Locale.ROOT, """
            Zero Length files      : %d
            Unreadable (skipped)   : %d
            Skipped (existing)     : %d
            Failed Conversions     : %d
            Successful Conversions : %d
            ================================
            """,
            getZeroFileCount(),
            getUnreadableCount(),
            getSkippedExistingCount(),
            getFailedConversionFileCount(),
            getSuccessfulConversionCount()
        ));
        return sb.toString();
    }
}
