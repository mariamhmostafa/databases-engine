package ML1;

import java.util.ArrayList;
import java.util.Vector;

public class Octree {
    private Vector<Octree> bbs = new Vector<>();
    private Vector<Point> points = new Vector<>();
    private Point topLeftFront;
    private Point bottomRightBack;


    public Octree(){

    }

    public Octree(Comparable xmin, Comparable ymin, Comparable zmin, Comparable xmax, Comparable ymax, Comparable zmax){
        topLeftFront = new Point(xmin, ymin, zmin);
        bottomRightBack = new Point(xmax, ymax, zmax);
    }

    public Vector<Octree> getBbs() {
        return bbs;
    }

    public void setBbs(Vector<Octree> bbs) {
        this.bbs = bbs;
    }

    public Vector<Point> getPoints() {
        return points;
    }

    public void setPoints(Vector<Point> points) {
        this.points = points;
    }
}
