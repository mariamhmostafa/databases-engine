package ML1;

import java.beans.Transient;
import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    String strTableName;
    String strClusteringKeyColumn;
    Hashtable<String,String> htblColNameType;
    Hashtable<String,String> htblColNameMin;
    Hashtable<String,String> htblColNameMax;
    Vector<String> pages=new Vector<>();
    private int pageCounter=0;

    public Table(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String>htblColNameMin,Hashtable<String,String> htblColNameMax){
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;
        this.htblColNameMin = htblColNameMin;
        this.htblColNameMax = htblColNameMax;
    }

}
//if within max and min: deserialize then binary search and then insert
//class properties