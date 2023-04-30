package ML1;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
public class Page implements Serializable {

    private int pageRows=1;
    private static int maxRows = Integer.parseInt(getVal("MaximumRowsCountinTablePage"));
    private Object maxValInPage;
    private Object minValInPage;
    private Vector<Tuple> tuplesInPage = new Vector<>();
    private String path;

    public Page(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public static void main(String[] args) {
        System.out.println(maxRows);
    }

    public static String getVal(String key)
    {
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
    public Object getMinValInPage() {
        return minValInPage;
    }

    public void setMinValInPage(Object minValInPage) {
        this.minValInPage = minValInPage;
    }

    public Vector<Tuple> getTuplesInPage() {
        return tuplesInPage;
    }

    public void setTuplesInPage(Vector<Tuple> tuplesInPage) {
        this.tuplesInPage = tuplesInPage;
    }

    public Object getMaxValInPage() {
        return maxValInPage;
    }

    public void setMaxValInPage(Object maxValInPage) {
        this.maxValInPage = maxValInPage;
    }
    public boolean isFull(){
        return pageRows == maxRows;
        //if( DBApp.MaximumRowsCountinTablePage)
    }

    public void createFile(){

    }
}
