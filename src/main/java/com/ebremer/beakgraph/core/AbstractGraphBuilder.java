package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.Params;
import java.io.File;
import java.util.List;

// T refers to the concrete class (e.g., HDF5Writer.Builder)
// Inputs are FILES (setSource / setSources): an in-memory Dataset must be
// written to TriG / N-Quads first. The former setDataset(Dataset) was a
// silent no-op no engine consumed (BG-264).
public abstract class AbstractGraphBuilder<T extends AbstractGraphBuilder<T>> {

    protected File src;
    protected File dest;
    protected List<File> sources = List.of();
    protected boolean spatial;
    protected boolean features;
    protected VoidMode voidMode = VoidMode.NONE;

    // Force the concrete class to return 'this'
    protected abstract T self();

    public T setSource(File file) {
        this.src = file;
        return self();
    }

    public T setDestination(File file) {
        this.dest = file;
        return self();
    }

    /**
     * Merge mode: every given document is parsed into the ONE store being
     * written (blank nodes stay distinct per document). When non-empty this
     * takes precedence over {@link #setSource}.
     */
    public T setSources(List<File> files) {
        this.sources = List.copyOf(files);
        return self();
    }

    public List<File> getSources() { return sources; }

    protected File sourceRoot;

    /**
     * Merge mode: the directory the merged documents' paths are taken relative
     * to when their relative references are stored (the CLI's {@code -src}).
     * Defaults to the sources' common ancestor directory.
     */
    public T setSourceRoot(File root) {
        this.sourceRoot = root;
        return self();
    }
    public File getSourceRoot() { return sourceRoot; }
    
    /**
     * Whether/how the VoID+SD statistics graph is generated: {@code NONE}
     * (default), {@code EXACT} (in-memory, CLI -void), or {@code SKETCH}
     * (bounded-memory HyperLogLog, CLI -voidsketch).
     */
    public T setVoidMode(VoidMode mode) {
        this.voidMode = mode;
        return self();
    }

    public VoidMode getVoidMode() {
        return voidMode;
    }

    protected String voidDatasetIri = Params.VOID_DATASET_IRI;

    /**
     * The IRI of the {@code sd:Dataset} resource the VoID/SD statistics graph
     * describes (CLI {@code -voidbase}); default {@link Params#VOID_DATASET_IRI}.
     * Every engine threads it into its statistics collector, so the six stay
     * isomorphic for one configuration (BG-109).
     */
    public T setVoidDatasetIri(String iri) {
        if (iri == null || iri.isBlank()) {
            throw new IllegalArgumentException("The VoID dataset IRI must not be blank");
        }
        this.voidDatasetIri = iri;
        return self();
    }

    public String getVoidDatasetIri() {
        return voidDatasetIri;
    }

    public T setSpatial(boolean flag) {
        this.spatial = flag;
        return self();
    }
    
    public T setFeatures(boolean flag) {
        this.features = flag;
        return self();
    }
    
    public boolean getSpatial() {
        return spatial;
    }
    
    public boolean getFeatures() {
        return features;
    }

    public File getSource() { return src; }
    public File getDestination() { return dest; }
    
    /**
     * The check every engine's {@code build()} runs first: without it the
     * six {@code write()} methods died on a bare NullPointerException from
     * {@code getDestination().toPath()} or {@code List.of(null)}, and the
     * disk engines had already created their workspace (BG-279).
     *
     * @throws IllegalStateException naming the missing setter
     */
    protected final void requireSourceAndDestination() {
        if (dest == null) {
            throw new IllegalStateException("No destination set: call setDestination()");
        }
        if (sources.isEmpty() && src == null) {
            throw new IllegalStateException("No source set: call setSource() or setSources()");
        }
    }

    // All writers usually need a root group name
    public abstract String getName(); 
    
    // All builders must produce a Writer
    public abstract BeakGraphWriter build() throws java.io.IOException;
}
