package com.ebremer.beakgraph.hdf5.jena;

/**
 * Utility class to calculate image scaling requirements.
 */
public class NumScale {

    /**
     * Calculates reduction steps using a loop (Iterative approach).
     * This is generally preferred for this use case because it avoids floating-point
     * precision errors entirely and N is very small (rarely > 10).
     * @param width
     */
    public static int calculateScaleSteps(int width, int height, int boxSize) {
        if (width <= 0 || height <= 0 || boxSize <= 0) {
            System.err.println("Error: Dimensions must be greater than 0.");
            return -1;
        }

        int steps = 0;
        double currentMaxDimension = Math.max(width, height);

        while (currentMaxDimension > boxSize) {
            currentMaxDimension = currentMaxDimension / 2.0;
            steps++;
        }

        return steps;
    }

    /**
     * Calculates reduction steps using Logarithms (Mathematical approach).
     * Formula: steps = ceil( log2(maxDimension / boxSize) )
     * * Note: This requires careful handling of floating point precision (epsilon)
     * to avoid off-by-one errors when the result is extremely close to an integer.
     */
    public static int calculateScaleStepsLog(int width, int height, int boxSize) {
        if (width <= 0 || height <= 0 || boxSize <= 0) return -1;
        
        int maxDim = Math.max(width, height);
        
        // If it already fits, 0 steps
        if (maxDim <= boxSize) return 0;

        // Calculate the ratio (e.g., 1024 / 256 = 4.0)
        double ratio = (double) maxDim / boxSize;

        // log2(x) = ln(x) / ln(2)
        double stepsExact = Math.log(ratio) / Math.log(2);

        // We subtract a tiny epsilon before ceiling to handle cases where 
        // floating point noise makes an exact integer result slightly higher.
        // e.g., if result is 3.00000000000004, ceil would return 4 without this.
        double epsilon = 1e-10;
        
        return (int) Math.ceil(stepsExact - epsilon);
    }

    public static void main(String[] args) {
        // Test comparing both methods
        testBoth(100, 100, 100);       // Fits exactly
        testBoth(1024, 768, 256);      // Power of 2 (1024 -> 512 -> 256), should be 2 steps
        testBoth(1000, 1000, 500);     // Exactly half (1000 -> 500), should be 1 step
        testBoth(1024, 1024, 1000);    // Edge case: Power of 2 slightly larger than box. 1024->512. 1 step.
    }

    private static void testBoth(int w, int h, int box) {
        int loopResult = calculateScaleSteps(w, h, box);
        int logResult = calculateScaleStepsLog(w, h, box);
        
        System.out.printf("Img [%d x %d] Box [%d] -> Loop: %d | Log: %d", w, h, box, loopResult, logResult);
        
        if (loopResult != logResult) {
            System.out.print(" [MISMATCH!]");
        }
        System.out.println();
    }
}