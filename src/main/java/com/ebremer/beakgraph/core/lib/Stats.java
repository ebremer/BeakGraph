package com.ebremer.beakgraph.core.lib;

import static com.ebremer.beakgraph.utils.UTIL.MinBits;

/**
 *
 * @author Erich Bremer
 */
public class Stats {
    public long numGraphs = 0;
    public long numSubjects = 0;
    public long numPredicates = 0;
    public long numObjects = 0;
    public long numShared = 0;
    public long numBlankNodes = 0;
    public long numIRI = 0;
    public long maxLong = Long.MIN_VALUE;
    public long minLong = Long.MAX_VALUE;
    public long numLong = 0;
    public int maxInteger = Integer.MIN_VALUE;
    public int minInteger = Integer.MAX_VALUE;
    public long numInteger = 0;
    public float maxFloat = Float.MIN_VALUE;
    public float minFloat = Float.MAX_VALUE;
    public long numFloat = 0;    
    public double maxDouble = Double.MIN_VALUE;
    public double minDouble = Double.MAX_VALUE;
    public long numDouble = 0;
    
    public long numStrings = 0;
    public int longestStringLength = Integer.MIN_VALUE;
    public int shortestStringLength = Integer.MAX_VALUE;
    
    @Override
    public String toString() {
        return String.format(
            """
            ==================================================================
            Number of Graphs      : %d
            Number of Subjects    : %d
            Number of Predicates  : %d
            Number of Objects     : %d
            Number of Shared      : %d
            Number of Blank nodes : %d
            
            Number of Integers    : %d
            Number of Longs       : %d
            Number of Floats      : %d
            Number of Doubles     : %d                                    
            
            MaxInteger            : %d
            MaxLong               : %d
            
            MinInteger            : %d
            MinLong               : %d
            
            MaxBitsInteger        : %d
            MaxBitsLong           : %d
            
            numStrings            : %d
            longestStringLength   : %d
            shortestStringLength  : %d
            ==================================================================
            """,
            numGraphs,
            numSubjects,
            numPredicates,
            numObjects,
            numShared,
            numBlankNodes,
            numInteger,
            numLong,
            numFloat,
            numDouble,
            maxInteger,
            maxLong,
            minInteger,
            minLong,
            MinBits( maxInteger ),
            MinBits( maxLong ),
            numStrings,
            longestStringLength,
            shortestStringLength
        );
    }
}
