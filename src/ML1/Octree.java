package ML1;

import java.io.File;
import java.io.FileInputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;


public class Octree {
    private Octree[] bbs = new Octree[8];
    private Vector<Point> points = new Vector<>();
    private Point topLeftFront;
    private Point bottomRightBack;
    private static int maxEntries = Integer.parseInt(getVal("MaximumEntriesinOctreeNode"));

    public Octree(){

    }

    //          r
    //   c c c c c c c c
    // 8  8  8  8 8 8 8 8
    // if points size < max insert

    public Octree(Comparable xmin, Comparable ymin, Comparable zmin, Comparable xmax, Comparable ymax, Comparable zmax){
        topLeftFront = new Point(xmin, ymin, zmin);
        bottomRightBack = new Point(xmax, ymax, zmax);
    }

    public Octree[] getBbs() { return bbs; }

    public void setBbs(Octree[] bbs) { this.bbs = bbs; }

    public Vector<Point> getPoints() {
        return points;
    }

    public void setPoints(Vector<Point> points) {
        this.points = points;
    }

//    public boolean isLeaf(){
//        return bbs.isEmpty();
//    }

    public void insert(Comparable x, Comparable y, Comparable z, int pageNum) throws DBAppException {
        if(x.compareTo(topLeftFront.x)<0  || x.compareTo(bottomRightBack.x)>0 ||
                y.compareTo(topLeftFront.y)<0  || y.compareTo(bottomRightBack.y)>0 ||
                z.compareTo(topLeftFront.z)<0  || z.compareTo(bottomRightBack.z)>0){
            throw new DBAppException("Out of range");
        }
        if(points.size()<maxEntries){ //if size less than max entries then insert
            points.add(new Point(x,y,z, pageNum));
            return;
        }
        
        Comparable midx = getMid(topLeftFront.x, bottomRightBack.x); //gets median of every dimension
        Comparable midy = getMid(topLeftFront.y, bottomRightBack.y);
        Comparable midz = getMid(topLeftFront.z, bottomRightBack.z);
        
        //Comparable newminx, newminy, newminz, newmaxx, newmaxy, newmaxz;
        
        int pos = getPos(x, y, z, midx, midy, midz);
        
        if(bbs[pos]==null){ //bb not initialized so we should create a new octree in that poistion
            Comparable[] newBounds = getNewBounds(midx, midy, midz, pos);
            bbs[pos] = new Octree(newBounds[0], newBounds[1], newBounds[2], newBounds[3], newBounds[4], newBounds[5]);
        }
        
        bbs[pos].insert(x,y,z,pageNum);
        
    }
    
    public Comparable[] getNewBounds(Comparable midx, Comparable midy, Comparable midz, int pos){
        Comparable[] newBounds = new Comparable[6];
        switch(pos){
            case 0:
                newBounds[0] = topLeftFront.x;
                newBounds[1] = topLeftFront.y;
                newBounds[2] = topLeftFront.z;
                newBounds[3] = midx;
                newBounds[4] = midy;
                newBounds[5] = midz;
                break;
            case 1:
                newBounds[0] = topLeftFront.x;
                newBounds[1] = topLeftFront.y;
                newBounds[2] = midz;
                newBounds[3] = midx;
                newBounds[4] = midy;
                newBounds[5] = bottomRightBack.z;
                break;
            case 2:
                newBounds[0] = topLeftFront.x;
                newBounds[1] = midy;
                newBounds[2] = topLeftFront.z;
                newBounds[3] = midx;
                newBounds[4] = bottomRightBack.y;
                newBounds[5] = midz;
                break;
            case 3:
                newBounds[0] = topLeftFront.x;
                newBounds[1] = midy;
                newBounds[2] = midz;
                newBounds[3] = midx;
                newBounds[4] = bottomRightBack.y;
                newBounds[5] = bottomRightBack.z;
                break;
            case 4:
                newBounds[0] = midx;
                newBounds[1] = topLeftFront.y;
                newBounds[2] = topLeftFront.z;
                newBounds[3] = bottomRightBack.x;
                newBounds[4] = midy;
                newBounds[5] = midz;
                break;
            case 5:
                newBounds[0] = midx;
                newBounds[1] = topLeftFront.y;
                newBounds[2] = midz;
                newBounds[3] = bottomRightBack.x;
                newBounds[4] = midy;
                newBounds[5] = bottomRightBack.z;
                break;
            case 6:
                newBounds[0] = midx;
                newBounds[1] = midy;
                newBounds[2] = topLeftFront.z;
                newBounds[3] = bottomRightBack.x;
                newBounds[4] = bottomRightBack.y;
                newBounds[5] = midz;
                break;
            default:
                newBounds[0] = midx;
                newBounds[1] = midy;
                newBounds[2] = midz;
                newBounds[3] = bottomRightBack.x;
                newBounds[4] = bottomRightBack.y;
                newBounds[5] = bottomRightBack.z;
                break;
        }
        return newBounds;
    }
    
    public static int getPos(Comparable x, Comparable y, Comparable z, Comparable midx, Comparable midy, Comparable midz){
        if(x.compareTo(midx)<=0){
            if(y.compareTo(midy)<=0){
                if(z.compareTo(midz)<=0){
                    return 0;
                }else{
                    return 1;
                }
            }else{
                if(z.compareTo(midz)<=0){
                    return 2;
                }else{
                    return 3;
                }
            }
        }else{
            if(y.compareTo(midy)<=0){
                if(z.compareTo(midz)<=0){
                    return 4;
                }else{
                    return 5;
                }
            }else{
                if(z.compareTo(midz)<=0){
                    return 6;
                }else{
                    return 7;
                }
            }
        }
    }
    
    public static Comparable getMid(Comparable min, Comparable max){
        if(min instanceof Integer){
            return ((int)min + (int)max )/2;
        }
        if(min instanceof Double){
            return ((double)min + (double)max )/2;
        }
        if(min instanceof String){
            return getMiddleString((String)min,(String) max);
        }
//        Calendar cal1 = Calendar.getInstance();
//        cal1.setTime((Date)min);
//        Calendar cal2 = Calendar.getInstance();
//        cal2.setTime((Date)max);
//        long diffInMillis = cal2.getTimeInMillis() - cal1.getTimeInMillis();
//        Calendar middleCal =  Calendar.getInstance();
//        middleCal.setTimeInMillis(cal1.getTimeInMillis() + diffInMillis / 2);
//        return middleCal.getTime();
        return new Date((((Date)min).getTime() +((Date)max).getTime())/2);
        
    }

    static String getMiddleString(String S, String T){
        int N = S.length();
        if(T.length()>S.length()){
            S += T.substring(S.length());
            N = T.length();
        }else if(T.length()<S.length()){
            T += S.substring(T.length());
        }
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;
        }
        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
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
            a1[i] = (int)a1[i] / 2;
        }
        String s ="";
        for (int i = 1; i <= N; i++) {
            s += (char)(a1[i] + 97);
        }
        return s;
    }

    public static String getVal(String key) {
        String keyval = null;
        //Let's consider properties file is in project folder itself

        File file = new File("src/Resources/DBApp.config");

        //Creating properties object
        Properties prop = new Properties();
        //Creating InputStream object to read data
        FileInputStream objInput = null;
        try{
            objInput = new FileInputStream(file);
            //Reading properties key/values in file
            prop.load(objInput);
            keyval = prop.getProperty(key);
            objInput.close();
        }catch(Exception e){System.out.println(e.getMessage());}
        return keyval;
    }

}
