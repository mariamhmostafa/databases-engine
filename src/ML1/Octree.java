package ML1;

import java.io.*;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;


public class Octree implements Serializable {
    private String[]columns=new String[3];
    private static Hashtable<String,Integer> htblColumns= new Hashtable<String,Integer>();
    private Octree[] bbs = new Octree[8];
    private String[] bbsPaths = new String[8];
    private Vector<Point> points = new Vector<>();
    private Point topLeftFront;
    private Point bottomRightBack;
    private static int maxEntries = Integer.parseInt(getVal("MaximumEntriesinOctreeNode"));
    private static int octreeCount =0;

    private boolean isLeaf = true;

    public Octree() {

    }

    //          r
    //   c c c c c c c c
    // 8  8  8  8 8 8 8 8
    // if points size < max insert

    public Octree(Comparable xmin, Comparable ymin, Comparable zmin, Comparable xmax, Comparable ymax, Comparable zmax, String x, String y, String z) {
        topLeftFront = new Point(xmin, ymin, zmin);
        bottomRightBack = new Point(xmax, ymax, zmax);
        htblColumns.put(x,0);
        htblColumns.put(y,1);
        htblColumns.put(z,2);
        columns[0]=x;
        columns[1]=y;
        columns[2]=z;
    }

    public Octree[] getBbs() {
        return bbs;
    }

    public void setBbs(Octree[] bbs) {
        this.bbs = bbs;
    }

    public Vector<Point> getPoints() {
        return points;
    }
    
    public void setPoints(Vector<Point> points) {
        this.points = points;
    }
    
    public void insert(Comparable x, Comparable y, Comparable z, String path, Object clustringkey) throws DBAppException {
        if(isLeaf && points.size()<maxEntries){
            //if size less than max entries then insert
            Point newpoint=new Point(x,y,z, path,clustringkey);
            for (Point p:points){
                if(newpoint.equals(p)){
                    p.getReference().put(clustringkey,path);
                    return;
                }
            }
            points.add(newpoint);
            return;
        }
        
        Comparable midx = getMid(topLeftFront.getX(), bottomRightBack.getX()); //gets median of every dimension
        Comparable midy = getMid(topLeftFront.getY(), bottomRightBack.getY());
        Comparable midz = getMid(topLeftFront.getZ(), bottomRightBack.getZ());
        //Comparable newminx, newminy, newminz, newmaxx, newmaxy, newmaxz;
        int pos = getPos(x, y, z, midx, midy, midz);
        if (isLeaf){
            isLeaf = false;
            for (int i = 0; i < bbs.length; i++) {
                octreeCount++;
                bbsPaths[i] = "src/Resources/" + "bbsPaths"+ octreeCount+ ".ser";
                Comparable[] newBounds = getNewBounds(midx, midy, midz, i);
                bbs[i] = new Octree(newBounds[0], newBounds[1], newBounds[2], newBounds[3], newBounds[4], newBounds[5], this.getColumns()[0], this.getColumns()[1], this.getColumns()[2]);
            }
            for (Point p : points) {
                int rePos = getPos(p.getX(), p.getY(), p.getZ(), midx, midy, midz);
                for (Object primarykey : p.getReference().keySet()) {
                    bbs[rePos].insert(p.getX(), p.getY(), p.getZ(), p.getReference().get(primarykey), primarykey);
                }
            }
        }
        bbs[pos].insert(x, y, z, path, clustringkey);
        for(int i=0; i<bbs.length;i++){
            serializeObject(bbs[i], bbsPaths[i]);
        }
    }

    public Comparable[] getNewBounds(Comparable midx, Comparable midy, Comparable midz, int pos) {
        Comparable[] newBounds = new Comparable[6];
        switch (pos) {
            case 0:
                newBounds[0] = topLeftFront.getX();
                newBounds[1] = topLeftFront.getY();
                newBounds[2] = topLeftFront.getZ();
                newBounds[3] = midx;
                newBounds[4] = midy;
                newBounds[5] = midz;
                break;
            case 1:
                newBounds[0] = topLeftFront.getX();
                newBounds[1] = topLeftFront.getY();
                newBounds[2] = midz;
                newBounds[3] = midx;
                newBounds[4] = midy;
                newBounds[5] = bottomRightBack.getZ();
                break;
            case 2:
                newBounds[0] = topLeftFront.getX();
                newBounds[1] = midy;
                newBounds[2] = topLeftFront.getZ();
                newBounds[3] = midx;
                newBounds[4] = bottomRightBack.getY();
                newBounds[5] = midz;
                break;
            case 3:
                newBounds[0] = topLeftFront.getX();
                newBounds[1] = midy;
                newBounds[2] = midz;
                newBounds[3] = midx;
                newBounds[4] = bottomRightBack.getY();
                newBounds[5] = bottomRightBack.getZ();
                break;
            case 4:
                newBounds[0] = midx;
                newBounds[1] = topLeftFront.getY();
                newBounds[2] = topLeftFront.getZ();
                newBounds[3] = bottomRightBack.getX();
                newBounds[4] = midy;
                newBounds[5] = midz;
                break;
            case 5:
                newBounds[0] = midx;
                newBounds[1] = topLeftFront.getY();
                newBounds[2] = midz;
                newBounds[3] = bottomRightBack.getX();
                newBounds[4] = midy;
                newBounds[5] = bottomRightBack.getZ();
                break;
            case 6:
                newBounds[0] = midx;
                newBounds[1] = midy;
                newBounds[2] = topLeftFront.getZ();
                newBounds[3] = bottomRightBack.getX();
                newBounds[4] = bottomRightBack.getY();
                newBounds[5] = midz;
                break;
            default:
                newBounds[0] = midx;
                newBounds[1] = midy;
                newBounds[2] = midz;
                newBounds[3] = bottomRightBack.getX();
                newBounds[4] = bottomRightBack.getY();
                newBounds[5] = bottomRightBack.getZ();
                break;
        }
        return newBounds;
    }

    public static int getPos(Comparable x, Comparable y, Comparable z, Comparable midx, Comparable midy, Comparable midz) {
        if (compareTo(x, midx) <= 0) {
            if (compareTo(y, midy) <= 0) {
                if (compareTo(z, midz) <= 0) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (compareTo(z, midz) <= 0) {
                    return 2;
                } else {
                    return 3;
                }
            }
        } else {
            if (compareTo(y, midy) <= 0) {
                if (compareTo(z, midz) <= 0) {
                    return 4;
                } else {
                    return 5;
                }
            } else {
                if (compareTo(z, midz) <= 0) {
                    return 6;
                } else {
                    return 7;
                }
            }
        }
    }
  
    public Comparable getMid(Comparable min, Comparable max){
        if(min instanceof Integer){
            return ((int)min + (int)max )/2;
        }
        if (min instanceof Double) {
            return ((double) min + (double) max) / 2;
        }
        if (min instanceof String) {
            return getMiddleString((String) min, (String) max);
        }
        return new Date((((Date)min).getTime() + ((Date)max).getTime())/2);
    }

    static String getMiddleString(String S, String T) {
        int N = S.length();
        if (T.length() > S.length()) {
            S += T.substring(S.length());
            N = T.length();
        } else if (T.length() < S.length()) {
            T += S.substring(T.length());
        }
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }
        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }
        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {
            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }
            a1[i] = (int) a1[i] / 2;
        }
        StringBuilder s = new StringBuilder();
        for (int i = 1; i <= N; i++) {
            s.append((char) (a1[i] + 97));
        }
        return s.toString();
    }

    public static String getVal(String key) {
        String keyval = null;
        //Let's consider properties file is in project folder itself

        File file = new File("src/Resources/DBApp.config");

        //Creating properties object
        Properties prop = new Properties();
        //Creating InputStream object to read data
        FileInputStream objInput = null;
        try {
            objInput = new FileInputStream(file);
            //Reading properties key/values in file
            prop.load(objInput);
            keyval = prop.getProperty(key);
            objInput.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return keyval;
    }

    public static int compareTo(Comparable x, Comparable y) {
        if (x instanceof java.lang.String)
            return ((String) x).compareTo((String) y);
        else if (x instanceof java.lang.Double)
            return ((Double) x).compareTo((Double) y);
        else if (x instanceof java.lang.Integer)
            return ((Integer) x).compareTo((Integer) y);
        else
            return ((Date) x).compareTo((Date) y);
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public String[] getColumns() {
        return columns;
    }

    public void updatePath(Comparable x, Comparable y, Comparable z, Object clustringkey, String path) {
        Point point = new Point(x, y, z);
        if (isLeaf) {
            for (Point p : points) {
                if (p.equals(point)) {
                    p.getReference().put(clustringkey, path);
                }
            }
        } else {
            for (int i = 0; i < bbs.length; i++) {
                bbs[i].updatePath(x, y, z, clustringkey, path);
            }   
        }
    }

    public void select(SQLTerm[] sqlTerm,int[] arr,Hashtable<Object,String>res) {//we should enter the columns in the correct order of the octree columns


        Object valuex = sqlTerm[arr[1]]._objValue;
        Object valuey= sqlTerm[arr[2]]._objValue;
        Object valuez=sqlTerm[arr[3]]._objValue;
        Comparable midx= getMid(topLeftFront.getX(), bottomRightBack.getX());
        Comparable midy= getMid(topLeftFront.getY(), bottomRightBack.getY());
        Comparable midz= getMid(topLeftFront.getZ(), bottomRightBack.getZ());

        ArrayList<Integer>xchildren=new ArrayList<>();
        ArrayList<Integer>ychildren=new ArrayList<>();
        ArrayList<Integer>zchildren=new ArrayList<>();
            if(!isLeaf) {
                switch (sqlTerm[arr[1]]._strOperator) {
                    case ">":

                        if (compareTo((Comparable) valuex, midx) > 0) {
                            xchildren.add(4);
                            xchildren.add(5);
                            xchildren.add(6);
                            xchildren.add(7);
                        }
                        else {//if = call get pos?
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                            xchildren.add(4);
                            xchildren.add(5);
                            xchildren.add(6);
                            xchildren.add(7);
                        }
                        break;
                    case ">="://no matter the value compared to mid always have to search in them all

                        xchildren.add(0);
                        xchildren.add(1);
                        xchildren.add(2);
                        xchildren.add(3);
                        xchildren.add(4);
                        xchildren.add(5);
                        xchildren.add(6);
                        xchildren.add(7);
                        break;
                    case "<":

                        if (compareTo((Comparable) valuex, midx) <= 0) {
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                        }else{
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                            xchildren.add(4);
                            xchildren.add(5);
                            xchildren.add(6);
                            xchildren.add(7);
                        }
                    case "<=":

                        if (compareTo((Comparable) valuex, midx) <= 0) {
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                        }else{
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                            xchildren.add(4);
                            xchildren.add(5);
                            xchildren.add(6);
                            xchildren.add(7);
                        }
                        break;
                    case "!=":

                        xchildren.add(0);
                        xchildren.add(1);
                        xchildren.add(2);
                        xchildren.add(3);
                        xchildren.add(4);
                        xchildren.add(5);
                        xchildren.add(6);
                        xchildren.add(7);
                        break;
                    default:

                        if (compareTo((Comparable) valuex, midx) <= 0) {
                            xchildren.add(0);
                            xchildren.add(1);
                            xchildren.add(2);
                            xchildren.add(3);
                        }else{
                            xchildren.add(4);
                            xchildren.add(5);
                            xchildren.add(6);
                            xchildren.add(7);
                        }


                }
                switch (sqlTerm[arr[2]]._strOperator) {
                    case ">":

                        if (compareTo((Comparable) valuey, midy) > 0) {
                            ychildren.add(2);
                            ychildren.add(3);
                            ychildren.add(6);
                            ychildren.add(7);
                        }
                        else{
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(2);
                            ychildren.add(3);
                            ychildren.add(4);
                            ychildren.add(5);
                            ychildren.add(6);
                            ychildren.add(7);
                        }
                        break;
                    case ">="://no matter the value compared to mid always have to search in them all

                       if( compareTo((Comparable) valuey, midy) > 0) {
                           ychildren.add(2);
                           ychildren.add(3);
                           ychildren.add(6);
                           ychildren.add(7);
                       }
                       else {
                           ychildren.add(0);
                           ychildren.add(1);
                           ychildren.add(2);
                           ychildren.add(3);
                           ychildren.add(4);
                           ychildren.add(5);
                           ychildren.add(6);
                           ychildren.add(7);
                       }
                        break;
                    case "<":

                        if (compareTo((Comparable) valuey, midy) <= 0) {
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(4);
                            ychildren.add(5);
                        }else{
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(2);
                            ychildren.add(3);
                            ychildren.add(4);
                            ychildren.add(5);
                            ychildren.add(6);
                            ychildren.add(7);
                        }
                    case "<=":

                        if (compareTo((Comparable) valuey, midy) <= 0) {
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(4);
                            ychildren.add(5);
                        }else{
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(2);
                            ychildren.add(3);
                            ychildren.add(4);
                            ychildren.add(5);
                            ychildren.add(6);
                            ychildren.add(7);
                        }
                        break;
                    case "!=":

                        ychildren.add(0);
                        ychildren.add(1);
                        ychildren.add(2);
                        ychildren.add(3);
                        ychildren.add(4);
                        ychildren.add(5);
                        ychildren.add(6);
                        ychildren.add(7);
                        break;
                    default:

                        if (compareTo((Comparable) valuey, midy) <= 0) {
                            ychildren.add(0);
                            ychildren.add(1);
                            ychildren.add(4);
                            ychildren.add(5);
                        }else{
                            ychildren.add(2);
                            ychildren.add(3);
                            ychildren.add(6);
                            ychildren.add(7);
                        }


                }
                switch (sqlTerm[arr[3]]._strOperator) {
                    case ">":

                        if (compareTo((Comparable) valuez, midz) > 0) {
                            zchildren.add(1);
                            zchildren.add(3);
                            zchildren.add(5);
                            zchildren.add(7);
                        }
                        else{
                            zchildren.add(0);
                            zchildren.add(1);
                            zchildren.add(2);
                            zchildren.add(3);
                            zchildren.add(4);
                            zchildren.add(5);
                            zchildren.add(6);
                            zchildren.add(7);
                        }
                        break;
                    case ">="://no matter the value compared to mid always have to search in them all

                        if (compareTo((Comparable) valuez, midz) > 0) {
                            zchildren.add(1);
                            zchildren.add(3);
                            zchildren.add(5);
                            zchildren.add(7);
                        }
                        else{
                            zchildren.add(0);
                            zchildren.add(1);
                            zchildren.add(2);
                            zchildren.add(3);
                            zchildren.add(4);
                            zchildren.add(5);
                            zchildren.add(6);
                            zchildren.add(7);
                        }
                        break;
                    case "<":

                        if (compareTo((Comparable) valuez, midz) <= 0) {
                            zchildren.add(0);
                            zchildren.add(2);
                            zchildren.add(4);
                            zchildren.add(6);
                        }else{
                            zchildren.add(0);
                            zchildren.add(1);
                            zchildren.add(2);
                            zchildren.add(3);
                            zchildren.add(4);
                            zchildren.add(5);
                            zchildren.add(6);
                            zchildren.add(7);
                        }
                    case "<=":

                        if (compareTo((Comparable) valuez, midz) <= 0) {
                            zchildren.add(0);
                            zchildren.add(2);
                            zchildren.add(4);
                            zchildren.add(6);
                        }else{
                            zchildren.add(0);
                            zchildren.add(1);
                            zchildren.add(2);
                            zchildren.add(3);
                            zchildren.add(4);
                            zchildren.add(5);
                            zchildren.add(6);
                            zchildren.add(7);
                        }
                        break;
                    case "!=":

                        zchildren.add(0);
                        zchildren.add(1);
                        zchildren.add(2);
                        zchildren.add(3);
                        zchildren.add(4);
                        zchildren.add(5);
                        zchildren.add(6);
                        zchildren.add(7);
                        break;
                    default:

                        if (compareTo((Comparable) valuez, midz) <= 0) {
                            zchildren.add(0);
                            zchildren.add(2);
                            zchildren.add(4);
                            zchildren.add(6);
                        }else{
                            zchildren.add(1);
                            zchildren.add(3);
                            zchildren.add(5);
                            zchildren.add(7);
                        }


                }
                for(int i:xchildren){
                    if(ychildren.contains(i)&&zchildren.contains(i)){
                        bbs[i].select(sqlTerm, arr,res);
                    }
                }
            }else{

                for(Point p:points){
                    if(isSelected(p.getX(),sqlTerm[arr[1]]._objValue, sqlTerm[arr[1]]._strOperator)&&
                            isSelected(p.getY(),sqlTerm[arr[2]]._objValue, sqlTerm[arr[2]]._strOperator)&&
                            isSelected(p.getZ(),sqlTerm[arr[3]]._objValue, sqlTerm[arr[3]]._strOperator)){

                        for(Object ref:p.getReference().keySet()){
                            res.put(ref,p.getReference().get(ref));

                        }
                    }
                }
            }


    }

    public boolean isSelected(Object value,Object objValue,String operator){
        switch(operator){
            case ">":
                if(((Comparable)value).compareTo(objValue)>0){
                    return true;
                }
                return false;
            case ">=":
                if(((Comparable)value).compareTo(objValue)>=0){
                    return true;
                }
                return false;
            case "<":
                if(((Comparable)value).compareTo(objValue)<0){
                    return true;
                }
                return false;
            case "<=":
                if(((Comparable)value).compareTo(objValue)<=0){
                    return true;
                }
                return false;
            case "!=":
                if(value instanceof NullWrapper && objValue == null){
                    return false;
                }
                if(value instanceof NullWrapper || objValue == null){
                    return true;
                }
                if(((Comparable)value).compareTo(objValue)!=0){
                    return true;
                }
                return false;
            default:
                if(value instanceof NullWrapper && objValue == null){
                    return true;
                }
                if(value instanceof NullWrapper || objValue == null){
                    return false;
                }
                if(((Comparable)value).compareTo(objValue)==0){
                    return true;
                }
                return false;
        }

    }
  
    public Point getTopLeftFront(){return topLeftFront;}

    public Point getBottomRightBack(){return bottomRightBack;}

    public static Hashtable<String, Integer> getHtblColumns() {
        return htblColumns;
    }
    
    public String[] getBbsPaths() {
        return bbsPaths;
    }
    
    protected Object deserializeObject(String path) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Object o = objectIn.readObject();
            objectIn.close();
            fileIn.close();
            return o;
        }catch(IOException | ClassNotFoundException e){
            throw new DBAppException(e);
        }
    }
    
    protected void serializeObject(Object o, String path) throws DBAppException{
        try {
            FileOutputStream fileOut = new FileOutputStream(path);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(o);
            objectOut.close();
            fileOut.close();
        }catch(IOException e){
            throw new DBAppException(e);
        }
    }

}

