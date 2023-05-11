package ML1;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

public class Point implements Serializable {

private Comparable x,y,z;
//max/min?
//Tuple reference;
    ArrayList<String> pagePath;

    public Point(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public ArrayList<String> getPagePath() {
        return pagePath;
    }

    public Point(Comparable x, Comparable y, Comparable z, String path){
        pagePath = new ArrayList<>();
        this.x = x;
        this.y = y;
        this.z = z;
        insert(path);
    }
    
    public void insert(String path){
        pagePath.add(path);
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
