package ML1;

public class Point{

Comparable x,y,z;
//max/min?
//Tuple reference;
    int pageNum;

    public Point(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Point(Comparable x, Comparable y, Comparable z,int pageNum){
        this.pageNum = pageNum;
        this.x = x;
        this.y = y;
        this.z = z;
    }

}
