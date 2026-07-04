package com.ebremer.beakgraph.core;

/**
 * How (and whether) a writer generates the VoID/SD statistics graph
 * ({@code urn:x-beakgraph:void}).
 *
 * <ul>
 * <li>{@link #NONE} - default: no statistics graph is written. Smallest,
 *     fastest builds; readers fall back to a fixed join-reorder heuristic.</li>
 * <li>{@link #EXACT} (CLI {@code -void}) - fully in-memory, exact counts;
 *     RAM grows with the number of distinct terms per graph.</li>
 * <li>{@link #SKETCH} (CLI {@code -voidsketch}) - bounded memory: exact up
 *     to 65,536 distinct nodes per counter, then HyperLogLog estimates
 *     (~0.8% error, deterministic). The right choice for disk-based builds
 *     that still want statistics-driven query optimization.</li>
 * </ul>
 */
public enum VoidMode {
    NONE,
    EXACT,
    SKETCH
}
