package com.ebremer.halcyon.hilbert;

/**
 *
 * @author Erich Bremer
 */
public class GridCell {
    public short scale;
    public final int x;
    public final int y;

    public GridCell(short scale, int x, int y) {
        this.scale = scale;
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return String.format("%d/%d/%d/", scale, x, y);
    }
}
