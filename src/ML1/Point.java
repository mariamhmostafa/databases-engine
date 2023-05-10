package ML1;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Point{

private Comparable x,y,z;
//max/min?
//Tuple reference;
    ArrayList<Integer> pageNums;

    public Point(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public ArrayList<Integer> getPageNums() {
        return pageNums;
    }

    public Point(Comparable x, Comparable y, Comparable z, int pageNum){
        pageNums = new ArrayList<>();
        this.x = x;
        this.y = y;
        this.z = z;
        insert(pageNum);
    }
    
    public void insert(int pageNum){
        pageNums.add(pageNum);
    }

    public Comparable getX(){
        return x;
    }

    public Comparable getY(){
        return y;
    }

    public Comparable getZ(){
        return z;
    }

    @Override
    public boolean equals(Object obj) {
        return (this.x).equals(((Point)obj).x)&&(this.y).equals(((Point)obj).y) &&(this.z).equals(((Point)obj).z);

    }
}
