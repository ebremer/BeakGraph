package com.ebremer.beakgraph.core;

import java.util.Locale;

/**
 * The ONE rule for "is this file name a BeakGraph store": {@code .h5} or
 * {@code .hdf5}, case-insensitively. {@code -verify} accepted both while
 * {@code -export} and the LWS servlet accepted {@code .h5} only, so a tree
 * that verified as fully OK exported a subset with no message (BG-159,
 * BG-319).
 *
 * @author Erich Bremer
 */
public final class BeakGraphFiles {

    private BeakGraphFiles() {}

    public static boolean isBeakGraphFileName(String name) {
        if (name == null) {
            return false;
        }
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.endsWith(".h5") || lower.endsWith(".hdf5");
    }

    /** The name without its {@code .h5} / {@code .hdf5} suffix; unchanged when it has neither. */
    public static String stripExtension(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".hdf5")) {
            return name.substring(0, name.length() - 5);
        }
        if (lower.endsWith(".h5")) {
            return name.substring(0, name.length() - 3);
        }
        return name;
    }
}
