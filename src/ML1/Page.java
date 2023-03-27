package ML1;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
public class Page implements Serializable {

    private int pageRows=0;
    private static int maxRows = Integer.parseInt(getVal("MaximumRowsCountinTablePage"));
    private transient int maxValInPage;
    private transient int minValInPage;
    private Vector<Tuple> tuplesInPage;
    private String path;

    public Page(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public Page (String tableName, int pageCounter){
        String fileName = tableName + pageCounter + ".bin";
        try {
            File myObj = new File(fileName);
            pageCounter++;
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
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

    public boolean isFull(){
        return pageRows == maxRows;
        //if( DBApp.MaximumRowsCountinTablePage)
    }

    public void createFile(){

    }
}
