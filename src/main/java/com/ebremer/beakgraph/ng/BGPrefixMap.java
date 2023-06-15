package com.ebremer.beakgraph.ng;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.riot.system.PrefixEntry;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.shared.PrefixMapping;

/**
 *
 * @author erich
 */
public class BGPrefixMap implements PrefixMap {

    @Override
    public String get(String prefix) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getMapping() {
        return new HashMap<>();
    }

    @Override
    public Map<String, String> getMappingCopy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void forEach(BiConsumer<String, String> action) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Stream<PrefixEntry> stream() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void add(String prefix, String iriString) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void putAll(PrefixMap pmap) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void putAll(PrefixMapping pmap) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void putAll(Map<String, String> mapping) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(String prefix) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsPrefix(String prefix) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String abbreviate(String uriStr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Pair<String, String> abbrev(String uriStr) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String expand(String prefixedName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String expand(String prefix, String localName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
