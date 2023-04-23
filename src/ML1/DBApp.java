package ML1;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

     public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException, ParseException{
         String strTableName = "Student";
         DBApp dbApp = new DBApp( );
         dbApp.init();
         //////creating table:
         Hashtable htblColNameType = new Hashtable();
         htblColNameType.put("id", "java.lang.Integer");
         htblColNameType.put("name", "java.lang.String");
         htblColNameType.put("gpa", "java.lang.double");
         Hashtable <String, String> colMin = new Hashtable<>();
         colMin.put("id", "0");
         colMin.put("name", "A");
         colMin.put("gpa", "0.0");
         Hashtable <String, String> colMax = new Hashtable<>();
         colMax.put("id", "100");
         colMax.put("name", "ZZZZZZZZZZZ");
         colMax.put("gpa","5.0");
         dbApp.createTable( strTableName, "id", htblColNameType, colMin, colMax);

         //////inserting into table:
         Hashtable htblColNameValue = new Hashtable( );
         htblColNameValue.put("id", new Integer( 19 ));
         htblColNameValue.put("name", new String("Mariam Maarek" ) );
         htblColNameValue.put("gpa", new Double( 0.87) );
         dbApp.insertIntoTable( strTableName , htblColNameValue );
         Hashtable htblColNameValue2 = new Hashtable( );
         htblColNameValue2.put("id", new Integer( 20 ));
         htblColNameValue2.put("name", new String("Nairuzy" ) );
         htblColNameValue2.put("gpa", new Double( 4.0) );
         dbApp.insertIntoTable( strTableName , htblColNameValue2 );
         Hashtable htblColNameValue3 = new Hashtable( );
         htblColNameValue3.put("id", new Integer( 22));
         htblColNameValue3.put("name", new String("Frfr" ) );
         htblColNameValue3.put("gpa", new Double( 4.0) );
         dbApp.insertIntoTable( strTableName , htblColNameValue3 );
//          System.out.println(table.getPaths().get(0));
//          dbApp.deleteFromTable(strTableName, toDelete);
         
         System.out.println("before:");
         Page page = (Page)dbApp.deserializeObject("src/Resources/Student0.ser");
         for(Tuple t: page.getTuplesInPage()){
             for(String key: t.getValues().keySet()){
                 System.out.println(key + " value: " + t.getValues().get(key).toString());
             }
         }
         dbApp.serializeObject(page, "src/Resources/Student0.ser");
         
         
         //////deleting from table:
         Hashtable toDelete = new Hashtable( );
         toDelete.put("gpa", new Double( 4.0 ) );
         dbApp.deleteFromTable(strTableName, toDelete);
         
         ////////updating table:
//         Hashtable htblColNameValue3 = new Hashtable( );
//         htblColNameValue3.put("gpa", new Double( 0.7) );
//         dbApp.updateTable(strTableName, "20", htblColNameValue3);
//
         System.out.println();
         System.out.println("after:");
         Page page1 = (Page) dbApp.deserializeObject("src/Resources/Student0.ser");
         for(Tuple t: page1.getTuplesInPage()){
             for(String key: t.getValues().keySet()){
                 System.out.println(key + " value: " + t.getValues().get(key).toString());
             }
         }
         dbApp.serializeObject(page1, "src/Resources/Student0.ser");
     }

    public void init() throws IOException {
        FileWriter writer = new  FileWriter  ( "src/Resources/metadata.csv", true );
        StringBuilder sb = new StringBuilder();
        sb.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max"+ "\n");
        writer.append(sb);
        writer.flush();
        writer.close();
    }


    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException {
        Table newTable = new Table(strTableName, strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
        serializeObject(newTable, "src/Resources/" + strTableName + ".ser");
        FileWriter writer = new  FileWriter  ( "src/Resources/metadata.csv", true );
        for(String colName : htblColNameType.keySet()){
            StringBuilder sb = new StringBuilder();
            String type = htblColNameType.get(colName);
            String min = htblColNameMin.get(colName);
            String max = htblColNameMax.get(colName);
            boolean clusteringKey = colName==strClusteringKeyColumn;
            sb.append(strTableName+", ");
            sb.append(colName+", ");
            sb.append(type+", ");
            sb.append(clusteringKey+", ");
            sb.append("null"+", ");
            sb.append("null"+", ");
            sb.append(min+", ");
            sb.append(max+ "\n");
            writer.append(sb);
        }
        writer.flush();
        writer.close();
    }

    public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ParseException, ClassNotFoundException {
        if(!isValid(strTableName, htblColNameValue)){
            throw new DBAppException("not valid :(");
        }     
        Table table = (Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        String primaryKey = table.getStrClusteringKeyColumn();
        Comparable value = (Comparable) htblColNameValue.get(primaryKey);
        Tuple newtuple = new Tuple(htblColNameValue, value);
        if(table.getPageCounter()==0){
            createPage(table, newtuple);
            serializeObject(table, "src/Resources/" + strTableName + ".ser");
            return;
        }
        int pathi = 0;
        String pathName = table.paths.get(pathi);
        Page page = (Page)deserializeObject(pathName);
        int i = getNewIndex(page.getTuplesInPage(), value);
        page.getTuplesInPage().add(i, newtuple);
        while(page.getTuplesInPage().size() > Integer.parseInt(Page.getVal("MaximumRowsCountinTablePage"))){
            Tuple lasttuple = page.getTuplesInPage().lastElement();
            if(pathi==table.getPageCounter()-1){
                createPage(table, lasttuple);
                serializeObject(table, "src/Resources/" + strTableName + ".ser");
                return;
            }
            pathName = table.paths.get(++pathi);
            deleteFromTable(strTableName, lasttuple.getValues());
            serializeObject(page, page.getPath());
            page = (Page)deserializeObject(pathName);
            i = getNewIndex(page.getTuplesInPage(), value);
            page.getTuplesInPage().add(i, newtuple);
        }
        serializeObject(page, page.getPath());
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
//     why   serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }

    public void createPage(Table table, Tuple newtuple) throws IOException {
        Page page = new Page("src/Resources/"+table.getStrTableName()+table.getPageCounter()+".ser");
        table.setPageCounter(table.getPageCounter()+1);
        page.setMinValInPage(newtuple.getPrimaryKey());
        page.setMaxValInPage(newtuple.getPrimaryKey());
        page.getTuplesInPage().add(newtuple);
        table.getPaths().add(page.getPath());
        serializeObject(page, page.getPath());
    }
    
    public int findIndex(Vector<Tuple> tuples, Comparable value) throws DBAppException { //NOT WORKING
        int low = 0;
        int high = tuples.size()-1;
        int mid=0;
        while(low<=high){
            mid = ((high-low)/2) + low;
            Comparable tupleVal = (Comparable)tuples.get(mid).getPrimaryKey();
            if(tupleVal.equals(value)){
                return mid;
            }else if(tupleVal.compareTo(value)<0){
                low = mid+1;
            }else{
                high = mid-1;
            }
        }
        return mid;
    }

    public int getNewIndex(Vector<Tuple> tuples, Comparable value) throws DBAppException { //NOT WORKING
        int low = 0;
        int high = tuples.size()-1;
        int mid=0;
        while(low<=high){
            mid = ((high-low)/2) + low; // 1 0 0
            Comparable tupleVal = (Comparable)tuples.get(mid).getPrimaryKey();
            if(tupleVal.equals(value)){
                throw new DBAppException("duplicate primary key");
            }else if(tupleVal.compareTo(value)<0){
                low = mid+1;
            }else{
                high = mid-1;
            }
        }
        return low;
    }

    public void updateTable(String strTableName,String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException, IOException, ClassNotFoundException, ParseException {
         if(!someAreValid(strTableName,htblColNameValue)) throw new DBAppException("Wrong values!");
        Table table=(Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        String primaryKeyName = table.getStrClusteringKeyColumn();
        FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        String row;
        String[] arr;
        Object clusteringKeyValue = null;
        while((row = br.readLine()) != null){
            arr = row.split(", ");
            if(arr[0].equals(strTableName)) {
                String colName = arr[1];
                String colType = arr[2].toLowerCase();
                if (colName.equalsIgnoreCase(primaryKeyName)) {
                    if(colType.equals("java.lang.integer")){
                       clusteringKeyValue= Integer.parseInt(strClusteringKeyValue);
                    }else if(colType.equals("java.lang.string")){
                         clusteringKeyValue= strClusteringKeyValue;
                    }else if(colType.equals("java.util.date")){
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        clusteringKeyValue= formatter.parse(strClusteringKeyValue);
                    }else if(colType.equals("java.lang.double")){
                        clusteringKeyValue= Double.parseDouble(strClusteringKeyValue);
                    }
                    int indexInPage =-1;
                    for(String pathName : table.getPaths()){
                        Page page = (Page)deserializeObject(pathName);
                        Object max= page.getTuplesInPage().get(page.getTuplesInPage().size()-1).getPrimaryKey();
                        Object min= page.getTuplesInPage().get(0).getPrimaryKey();
                        if(clusteringKeyValue instanceof String){
                            if(((String)max).compareTo((String) clusteringKeyValue)>=0 && ((String)min).compareTo((String) clusteringKeyValue)<=0){
                                indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                            }
                        }else if(clusteringKeyValue instanceof Integer){
                            if(((Integer)max).compareTo((Integer) clusteringKeyValue)>=0 && ((Integer)min).compareTo((Integer) clusteringKeyValue)<=0){
                                indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                            }
                        }else if(clusteringKeyValue instanceof Double){
                            if(((Double)max).compareTo((Double) clusteringKeyValue)>=0 && ((Double)min).compareTo((Double) clusteringKeyValue)<=0){
                                indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                            }
                        }else{
                            if(((Date)max).compareTo((Date) clusteringKeyValue)>=0 && ((Date)min).compareTo((Date) clusteringKeyValue)<=0){
                                indexInPage = findIndex(page.getTuplesInPage(), (Comparable)clusteringKeyValue);
                            }
                        }
                        if(indexInPage!=-1){ //may update a different tuple if clustering key not found
                            updateInPage(page,indexInPage,htblColNameValue);
                            serializeObject(table, "src/Resources/" + strTableName + ".ser");
                            serializeObject(page, page.getPath());
                            return;
                        }
                        serializeObject(page, page.getPath());
                    }
                }
            }
        }
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        throw new DBAppException("Cannot update");
     }
     
    public void updateInPage(Page page,int index,Hashtable<String,Object> htblColNameValue) throws IOException {
        for(String key: htblColNameValue.keySet()){
            Object value = htblColNameValue.get(key);
            page.getTuplesInPage().get(index).getValues().put(key,value);
        }
    }
    
    public void deleteFromTable2(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        Table table = (Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        String primaryKeyName = table.getStrClusteringKeyColumn();
        Object primaryKey = htblColNameValue.get(primaryKeyName);
        int indexInPage =-1;
        for(String pathName : table.getPaths()){
            Page page = (Page)deserializeObject(pathName);
            Object max= page.getTuplesInPage().get(page.getTuplesInPage().size()-1).getPrimaryKey();
            Object min= page.getTuplesInPage().get(0).getPrimaryKey();
            if(primaryKey instanceof String){
                if(((String)max).compareTo((String) primaryKey)>=0 && ((String)min).compareTo((String) primaryKey)<=0){
                    indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else if(primaryKey instanceof Integer){
                if(((Integer)max).compareTo((Integer) primaryKey)>=0 && ((Integer)min).compareTo((Integer) primaryKey)<=0){
                    indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else if(primaryKey instanceof Double){
                if(((Double)max).compareTo((Double) primaryKey)>=0 && ((Double)min).compareTo((Double) primaryKey)<=0){
                    indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else{
                if(((Date)max).compareTo((Date) primaryKey)>=0 && ((Date)min).compareTo((Date) primaryKey)<=0){
                    indexInPage = findIndex(page.getTuplesInPage(), (Comparable)primaryKey);
                }
            }
            if(indexInPage!=-1){
                page.getTuplesInPage().remove(indexInPage);
                page.setMaxValInPage(page.getTuplesInPage().lastElement().getPrimaryKey());
                page.setMinValInPage(page.getTuplesInPage().firstElement().getPrimaryKey());
//                deleteFromPage(page, indexInPage, primaryKey, primaryKeyName);
                if(page.getTuplesInPage().isEmpty()){
                    deletePage(table, pathName);
                }
                serializeObject(table, "src/Resources/" + strTableName + ".ser");
                serializeObject(page, page.getPath());
                return;
            }
            serializeObject(page, page.getPath());
        }
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        throw new DBAppException("Tuple Not found");
    }

    public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        if(!someAreValid(strTableName,htblColNameValue)) throw new DBAppException("Wrong values");

        Table table = (Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        String primaryKeyName = table.getStrClusteringKeyColumn();
        Object primaryKey = htblColNameValue.get(primaryKeyName);
        if(primaryKey!=null) {deleteFromTable2(strTableName,htblColNameValue);}
        else{
            for(String path:table.getPaths()){
                Page page=(Page)deserializeObject(path);
                for(Tuple record:page.getTuplesInPage()){
                    boolean allConditionsMet=true;
                    for(String key: htblColNameValue.keySet()){
                        if(!(record.getValues().get(key).equals(htblColNameValue.get(key)))){
                            allConditionsMet=false;
                            break;
                        }
                    }
                    if(allConditionsMet){
                      page.getTuplesInPage().remove(record);
                    }
                }
                if(page.getTuplesInPage().isEmpty()){
                    deletePage(table,path);
                }else {
                    page.setMaxValInPage(page.getTuplesInPage().lastElement().getPrimaryKey());
                    page.setMinValInPage(page.getTuplesInPage().firstElement().getPrimaryKey());
                    serializeObject(page,page.getPath());
                }
            }
        }
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }
    
    public void deletePage(Table table, String pathName) throws IOException, ClassNotFoundException {
        table.getPaths().remove(pathName);
        Path path= Paths.get(pathName);
        Files.deleteIfExists(path);
        table.setPageCounter(table.getPageCounter()-1);
    }

    public boolean isValid(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException, ParseException {
        FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        String row = br.readLine();
        String[] arr;
        boolean foundTableName = false;
        int countOfCols = 0;
        while(row!=null){
            arr = row.split(", ");
            if(arr[0].equals(strTableName)){
                foundTableName = true;
                countOfCols++;
                String colName = arr[1];
                String colType = arr[2].toLowerCase();
                String min = arr[6];
                String max = arr[7];
                Object object = htblColNameValue.get(colName);

                if(colType.equals("java.lang.integer")){
                    if(!(object instanceof java.lang.Integer)){
                        return false;
                    }
                    int minI = Integer.parseInt(min);
                    int maxI = Integer.parseInt(max);
                    if(((Integer)object)<minI || ((Integer)object)>maxI){
                        return false;
                    }
                }else if(colType.equals("java.lang.string")){
                    if(!(object instanceof java.lang.String)){
                        return false;
                    }
                    if(((String)object).compareTo(min)<0 || ((String)object).compareTo(max)>0){
                        return false;
                    }
                }else if(colType.equals("java.util.date")){
                    if(!(object instanceof java.util.Date)){
                        return false;
                    }
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    Date minDate = formatter.parse(min);
                    Date maxDate = formatter.parse(max);
                    if(((Date)object).compareTo(minDate)<0 || ((Date)object).compareTo(maxDate)>0){
                        return false;
                    }
                }else if(colType.equals("java.lang.double")){
                    if(!(object instanceof java.lang.Double)){
                        return false;
                    }
                    double minD = Double.parseDouble(min);
                    double maxD = Double.parseDouble(max);
                    if(((Double)object)<minD || ((Double)object)>maxD){
                        return false;
                    }
                }
            }
            row = br.readLine();
        }
        if(countOfCols != htblColNameValue.size()){
            return false;
        }
        return foundTableName;
    }
    
    public boolean someAreValid(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException, ParseException {
        FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        String row;
        String[] arr;
        boolean foundTableName = false;
        while((row = br.readLine())!=null){
            arr = row.split(", ");
            if(arr[0].equals(strTableName)){
                foundTableName = true;
                String colName = arr[1];
                String colType = arr[2].toLowerCase();
                String min = arr[6];
                String max = arr[7];
                Object object = htblColNameValue.get(colName);
                if(object == null){
                    continue;
                }
                if(colType.equals("java.lang.integer")){
                    if(!(object instanceof java.lang.Integer)){
                        return false;
                    }
                    int minI = Integer.parseInt(min);
                    int maxI = Integer.parseInt(max);
                    if(((Integer)object)<minI || ((Integer)object)>maxI){
                        return false;
                    }
                }else if(colType.equals("java.lang.string")){
                    if(!(object instanceof java.lang.String)){
                        return false;
                    }
                    if(((String)object).compareTo(min)<0 || ((String)object).compareTo(max)>0){
                        return false;
                    }
                }else if(colType.equals("java.util.date")){
                    if(!(object instanceof java.util.Date)){
                        return false;
                    }
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    Date minDate = formatter.parse(min);
                    Date maxDate = formatter.parse(max);
                    if(((Date)object).compareTo(minDate)<0 || ((Date)object).compareTo(maxDate)>0){
                        return false;
                    }
                }else if(colType.equals("java.lang.double")){
                    if(!(object instanceof java.lang.Double)){
                        return false;
                    }
                    double minD = Double.parseDouble(min);
                    double maxD = Double.parseDouble(max);
                    if(((Double)object)<minD || ((Double)object)>maxD){
                        return false;
                    }
                }
            }
        }
        return foundTableName;
    }
    
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException{
        return null;
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


