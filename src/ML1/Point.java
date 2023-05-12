package ML1;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class Point implements Serializable {

private Comparable x,y,z;
//max/min?
//Tuple reference;
    private Hashtable<Object,String> reference= new Hashtable<>(); //clustering key and ???? path

    public Point(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Hashtable<Object,String> getReference() {
        return reference;
    }

    public Point(Comparable x, Comparable y, Comparable z, String path, Object clusteringKey){
        this.x = x;
        this.y = y;
        this.z = z;
        this.reference.put(clusteringKey,path);
    }
    
//    public void insert(String path) {
//        pagePath.add(path);
//    }


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
