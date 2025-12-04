package com.ebremer.beakgraph.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.file.Path;
import java.time.Duration;

/**
 *
 * @author erich
 */
public class CacheService {
    
    private static Cache<Path, BeakGraph> cache;
    
    public CacheService() {
        System.out.println("Starting Cache Service...");
        cache = Caffeine.newBuilder()
            .recordStats()
            .maximumSize(100000)
            .expireAfterAccess(Duration.ofMinutes(10))
            .build();
    }
    
    public static Cache<Path, BeakGraph> getCache() {
        return cache;
    }    
}
