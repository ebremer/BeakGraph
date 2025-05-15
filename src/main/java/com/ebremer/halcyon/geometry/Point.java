package com.ebremer.halcyon.geometry;

import java.util.Arrays;

/**
 *
 * @author erich
 */
public class Point {
    public int x;
    public int y;
    
    public Point() {
        this(0,0);
    }
    
   public Point(int x, int y) {
        this.x = x;
        this.y = y;
   }
   
   public int[] asIntegerArray() {
       return new int[] {x,y};
   }

   public long[] asLongArray() {
       return new long[] {x,y};
   }
   
   public static boolean isSamePoint(Point a, Point b) {
       return ((a.x==b.x)&&(a.y==b.y));
   }
   
   @Override
   public String toString() {
       return x+", "+y;
   }
   
   @Override
   public Point clone() {
       return new Point(x,y);
   }
   
   @Override
    public int hashCode() {
        int wow = Arrays.hashCode(new Object[]{x, y});
        //System.out.println(wow+" -> "+x+", "+y);
        return wow;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Point other = (Point) obj;
        if (this.x != other.x) {
            return false;
        }
        return this.y == other.y;
    }
    
    public static void main(String[] args) {
        Point a = new Point(122,213);
        Point b = new Point(122,213);
        Point c = new Point(22,113);
        System.out.println(a.equals(b));
        System.out.println(a.equals(c));
        System.out.println(b.equals(c));
    }
}
