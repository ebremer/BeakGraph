package com.ebremer.halcyon.geometry;

/**
 *
 * @author erich
 */
public class Vector2D {
    int a;
    int b;
    
    Vector2D(int x, int y) {
        a = x;
        b = y;
    }
    
    public double Magnitude() {
        // double math: int*int overflows for coordinates above ~46340, which
        // slide-scale coordinates routinely exceed.
        return Math.sqrt(((double) a * a) + ((double) b * b));
    }

    public static double Magnitude(long[] a) {
        double sum = 0;
        for (int c=0; c<a.length; c++) {
            sum = sum + ((double) a[c] * a[c]);
        }
        return Math.sqrt(sum);
    }

    public static double Magnitude(int[] a) {
        double sum = 0;
        for (int c=0; c<a.length; c++) {
            sum = sum + ((double) a[c] * a[c]);
        }
        return Math.sqrt(sum);
    }

    public static double Magnitude(Point a) {
        // Was sqrt(x*x + y+y): addition instead of multiplication, plus int overflow.
        return Math.sqrt(((double) a.x * a.x) + ((double) a.y * a.y));
    }
    
    public static Point SmallestMag(Point a, Point b) {
        if (Magnitude(a)<Magnitude(b)) {
            return a;
        } else {
            return b;
        }
    }
}
