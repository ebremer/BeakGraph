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
        return Math.sqrt((a*a)+(b*b));
    }
    
    public static double Magnitude(long[] a) {
        long sum = 0;
        for (int c=0; c<a.length; c++) {
            sum = sum + (a[c]*a[c]);
        }
        return Math.sqrt(sum);
    }
    
    public static double Magnitude(int[] a) {
        long sum = 0;
        for (int c=0; c<a.length; c++) {
            sum = sum + (a[c]*a[c]);
        }
        return Math.sqrt(sum);
    }

    public static double Magnitude(Point a) {
        return Math.sqrt((a.x*a.x)+(a.y+a.y));
    }
    
    public static Point SmallestMag(Point a, Point b) {
        if (Magnitude(a)<Magnitude(b)) {
            return a;
        } else {
            return b;
        }
    }
}
