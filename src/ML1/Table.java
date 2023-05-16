package ML1;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    String strTableName;
    String strClusteringKeyColumn;
    Hashtable<String,String> htblColNameType;
    Hashtable<String,String> htblColNameMin;
    Hashtable<String,String> htblColNameMax;
    Vector<String> paths =new Vector<>();
    private int pageCounter=0;
    private Hashtable<String, String> octrees = new Hashtable<>();

    public Table(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String>htblColNameMin,Hashtable<String,String> htblColNameMax){
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;
        this.htblColNameMin = htblColNameMin;
        this.htblColNameMax = htblColNameMax;
    }

    public Hashtable<String, String> getOctreePaths(){
        return octrees;
    }

    public String getStrTableName() {
        return strTableName;
    }

    public String getStrClusteringKeyColumn() {
        return strClusteringKeyColumn;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public Hashtable<String, String> getHtblColNameMin() {
        return htblColNameMin;
    }

    public Hashtable<String, String> getHtblColNameMax() {
        return htblColNameMax;
    }

    public Vector<String> getPaths() {
        return paths;
    }

    public int getPageCounter() {
        return pageCounter;
    }

    public void setPageCounter(int pageCounter) {
        this.pageCounter = pageCounter;
    }
}
