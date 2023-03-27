package ML1;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    Hashtable<String, Table> tables = new Hashtable<>();

    public void init() throws FileNotFoundException // this does whatever initialization you would like // or leave it empty if there is no code you want to
    {
        FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
        //FileWriter
    }

   public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException{
        Table newTable = new Table(strTableName, strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
        tables.put(strTableName, newTable);
    }

    //CSVWriter writer = new CSVWriter(new FileWriter("new.csv"));
    //writer.writeNext(lineAsList.toArray());


   public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ParseException {
       if(!isValid(strTableName, htblColNameValue)){
           throw new DBAppException("not valid :(");
       }


   }

   public void updateTable(String strTableName,String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException{

   }

   public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException{

   }
    
   public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException{
       return null;
   }

   public boolean isValid(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException, ParseException {
        FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        String row = br.readLine();
        String[] arr = row.split(",");
        boolean foundTableName = false;
        int countOfCols =0;
        while(row!=null){
            if(arr[0] == strTableName){
                foundTableName = true;
                countOfCols++;
                String colName = arr[1];
                String colType = arr[2].toLowerCase();
                String min = arr[6];
                String max = arr[7];
                Object object = htblColNameValue.get(colName);
                switch(colType){
                    case "java.lang.integer":
                        if(!(object instanceof Integer)){
                            return false;
                        }
                        int minI = Integer.parseInt(min);
                        int maxI = Integer.parseInt(max);
                        if(((Integer)object)<minI || ((Integer)object)>maxI){
                            return false;
                        }
                        break;
                    case "java.lang.string":
                        if(!(object instanceof String)){
                            return false;
                        }
                        if(((String)object).compareTo(min)<0 || ((String)object).compareTo(max)>0){
                            return false;
                        }
                        break;
                    case "java.util.date":
                        if(!(object instanceof Date)){
                            return false;
                        }
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        Date minDate = formatter.parse(min);
                        Date maxDate = formatter.parse(max);
                        if(((Date)object).compareTo(minDate)<0 || ((Date)object).compareTo(maxDate)>0){
                            return false;
                        }
                        break;
                    default:
                        if(!(object instanceof Double)){
                            return false;
                        }
                        double minD = Double.parseDouble(min);
                        double maxD = Double.parseDouble(max);
                        if(((Double)object)<minD || ((Double)object)>maxD){
                            return false;
                        }
                        break;
                }
            }
            row = br.readLine();
            arr = row.split(",");
        }
        if(countOfCols != htblColNameValue.size()){
            return false;
        }
        return foundTableName;
   }

   protected Object deserializeObject(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object o = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return o;
   }

    protected void serializeObject(Object o, String path) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(path);
        ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
        objectOut.writeObject(o);
        objectOut.close();
        fileOut.close();
    }
}


