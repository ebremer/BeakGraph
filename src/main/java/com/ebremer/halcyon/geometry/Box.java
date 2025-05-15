package com.ebremer.halcyon.geometry;

import com.ebremer.halcyon.geometry.Point;

/**
 *
 * @author erich
 */
public class Box {
    public int MinX;
    public int MinY;
    public int MaxX;
    public int MaxY;
    public Point TopLeft;
    
    public Box(int minx, int miny, int maxx, int maxy) {
        MinX = minx;
        MinY = miny;
        MaxX = maxx;
        MaxY = maxy;
        TopLeft = new Point(Integer.MAX_VALUE,Integer.MAX_VALUE);
    }
    
    @Override
    public String toString() {
        return "Bounding Box === > Min : ("+MinX+","+MinY+") Max : ("+MaxX+","+MaxY+")\n"+TopLeft.toString();
    }
}
