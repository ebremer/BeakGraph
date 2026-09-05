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
    public long numDataTypes = 0;
    public long numShared = 0;
    public long numBlankNodes = 0;
    public long numIRI = 0;
    public long maxLong = Long.MIN_VALUE;
    public long minLong = Long.MAX_VALUE;
    public long numLong = 0;
    public int maxInteger = Integer.MIN_VALUE;
    public int minInteger = Integer.MAX_VALUE;
    public long numInteger = 0;
    // Seeds for running max must be the most NEGATIVE value: Float.MIN_VALUE /
    // Double.MIN_VALUE are the smallest POSITIVE values, which made the reported
    // max wrong for all-negative data.
    public float maxFloat = -Float.MAX_VALUE;
    public float minFloat = Float.MAX_VALUE;
    public long numFloat = 0;
    public double maxDouble = -Double.MAX_VALUE;
    public double minDouble = Double.MAX_VALUE;
    public long numDouble = 0;
    
    public long numStrings = 0;
    public int longestStringLength = Integer.MIN_VALUE;
    public int shortestStringLength = Integer.MAX_VALUE;

    // RDF 1.2 triple terms (distinct, all nesting depths). Drives allocation of
    // the literals section's tripleTerms component store.
    public long numTripleTerms = 0;

    /**
     * The bit width the dictionary writer allocates for the integers section:
     * 0 when there are none, 32 when a negative value forces the full
     * two's-complement pattern, otherwise a sign bit plus the bits of the
     * maximum (the ONE rule; MultiTypeDictionaryWriter allocates through it).
     */
    public int integerWidth() {
        if (numInteger == 0) return 0;
        return (minInteger < 0) ? 32 : 1 + MinBits(maxInteger);
    }

    /**
     * The bit width the dictionary writer allocates for the longs section: 0
     * when there are none, 64 for negatives, otherwise a sign bit plus the bits
     * of the maximum - rounded up to 64 past 57, the packed buffer's widest
     * unaligned width.
     */
    public int longWidth() {
        if (numLong == 0) return 0;
        int w = (minLong < 0) ? 64 : 1 + MinBits(maxLong);
        return (w > 57) ? 64 : w;
    }

    /** {@code value} when the section has entries, else "-": the seeds (MIN/MAX sentinels) are not data (BG-29). */
    private static String present(long count, Object value) {
        return (count > 0) ? String.valueOf(value) : "-";
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.ROOT,
            """
            ==================================================================
            Number of Graphs      : %d
            Number of Subjects    : %d
            Number of Predicates  : %d
            Number of Objects     : %d
            Number of Shared      : %d
            Number of Blank nodes : %d
            
            Number of Data Types  : %d
            
            Number of Integers    : %d
            Number of Longs       : %d
            Number of Floats      : %d
            Number of Doubles     : %d
            Number of Triple terms: %d

            MaxInteger            : %s
            MaxLong               : %s

            MinInteger            : %s
            MinLong               : %s

            IntegerWidth (bits)   : %d
            LongWidth (bits)      : %d

            numStrings            : %d
            longestStringLength   : %s
            shortestStringLength  : %s
            ==================================================================
            """,            numGraphs,
            numSubjects,
            numPredicates,
            numObjects,
            numShared,
            numBlankNodes,
            numDataTypes,
            numInteger,
            numLong,
            numFloat,
            numDouble,
            numTripleTerms,
            present(numInteger, maxInteger),
            present(numLong, maxLong),
            present(numInteger, minInteger),
            present(numLong, minLong),
            integerWidth(),
            longWidth(),
            numStrings,
            present(numStrings, longestStringLength),
            present(numStrings, shortestStringLength)
        );
    }
}
