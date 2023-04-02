package ML1;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    Vector<String> tableNames = new Vector<>();

    public void init() throws IOException
    {
        FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
        FileWriter writer = new  FileWriter  ( "src/Resources/metadata.csv", true );
        StringBuilder sb = new StringBuilder();
        sb.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max");
        writer.append(sb);
        writer.flush();
        writer.close();
    }


    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException {
        Table newTable = new Table(strTableName, strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
        tableNames.add(strTableName);
        serializeObject(newTable, "src/Resources/" + strTableName + ".ser");
        //write to csv
        FileWriter writer = new  FileWriter  ( "src/Resources/metadata.csv", true );
        StringBuilder sb = new StringBuilder();
        for(String colName : htblColNameType.keySet()){
            String type = htblColNameType.get(colName);
            String min = htblColNameMin.get(colName);
            String max = htblColNameMax.get(colName);
            boolean clusteringKey = colName==strClusteringKeyColumn;
            sb.append(strTableName);
            sb.append(colName);
            sb.append(type);
            sb.append(clusteringKey);
            sb.append("null");
            sb.append("null");
            sb.append(min);
            sb.append(max);
            writer.append(sb);
        }
        writer.flush();
        writer.close();
    }

    //CSVWriter writer = new CSVWriter(new FileWriter("new.csv"));
    //writer.writeNext(lineAsList.toArray());


    public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ParseException, ClassNotFoundException {
        if(!isValid(strTableName, htblColNameValue)){
            throw new DBAppException("not valid :(");
        }
        String primaryKey = getPrimaryKeyName(strTableName);
        Comparable value = (Comparable) htblColNameValue.get(primaryKey);
        Table table = (Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        Tuple newtuple = new Tuple(htblColNameValue, primaryKey);
        if(table.getPageCounter()==0){
            createPage(table, newtuple);
            serializeObject(table, "src/Resources/" + strTableName + ".ser");
            return;
        }

        int pathi = 0;
        String pathName = table.paths.get(pathi);
        Page page = (Page)deserializeObject(pathName);
            int i = getIndex(page.getTuplesInPage(), value);
            page.getTuplesInPage().add(i, newtuple); //inserts into first page regardless?
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
                i = getIndex(page.getTuplesInPage(), value);
                page.getTuplesInPage().add(i, newtuple);
            }
        serializeObject(page, page.getPath());
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }

    public void createPage(Table table, Tuple newtuple) throws IOException {
        Page page = new Page(table.getStrTableName()+table.getPageCounter()+".ser");
        table.setPageCounter(table.getPageCounter()+1);
        page.setMinValInPage(newtuple.getPrimaryKey());
        page.setMaxValInPage(newtuple.getPrimaryKey());
        page.getTuplesInPage().add(newtuple);
        serializeObject(page, page.getPath());
    }

    public void deletePage(Table table, String pathName) throws IOException, ClassNotFoundException {
        table.getPaths().remove(pathName);
        table.setPageCounter(table.getPageCounter()-1);
        //File file=new File()
        //delete file how??? O.o hi
    }

    public int getIndex(Vector<Tuple> tuples, Comparable value) throws DBAppException {
        int low = 0;
        int high = tuples.size()-1;
        int mid = (low+high)/2;
        while(low<high){
            Comparable tupleVal = (Comparable)tuples.get(mid).getPrimaryKey();
            if(tupleVal.equals(value)){
                throw new DBAppException("duplicate primary key");
            }else if(tupleVal.compareTo(value)<0){
                low = mid;
            }else{
                high = mid;
            }
            mid = (low+high)/2;
        }
        return mid;
    }

    public void updateTable(String strTableName,String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException{

    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        String primaryKeyName = getPrimaryKeyName(strTableName);
        Object primaryKey = htblColNameValue.get(primaryKeyName);
        Table table = (Table)deserializeObject("src/Resources/" + strTableName + ".ser");
        int indexInPage =-1;
        for(String pathName : table.getPaths()){
            Page page = (Page)deserializeObject(pathName);
            if(primaryKey instanceof String){
                if(((String)page.getMaxValInPage()).compareTo((String) primaryKey)>=0 && ((String)page.getMinValInPage()).compareTo((String) primaryKey)<=0){
                    indexInPage = getIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else if(primaryKey instanceof Integer){
                if(((Integer)page.getMaxValInPage()).compareTo((Integer) primaryKey)>=0 && ((Integer)page.getMinValInPage()).compareTo((Integer) primaryKey)<=0){
                    indexInPage = getIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else if(primaryKey instanceof Double){
                if(((Double)page.getMaxValInPage()).compareTo((Double) primaryKey)>=0 && ((Double)page.getMinValInPage()).compareTo((Double) primaryKey)<=0){
                    indexInPage = getIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                }
            }else{
                if(((Date)page.getMaxValInPage()).compareTo((Date) primaryKey)>=0 && ((Date)page.getMinValInPage()).compareTo((Date) primaryKey)<=0){
                    indexInPage = getIndex(page.getTuplesInPage(), (Comparable)primaryKey);
                }
            }
            if(indexInPage!=-1){
                int deletePage = deleteFromPage(page, indexInPage, primaryKey, primaryKeyName);
                if(deletePage == -1){
                    deletePage(table, pathName);
                }
                return;
            }
        }
        throw new DBAppException(); //tuple doesn't exist or table is empty
    }
    public int deleteFromPage(Page page,int index,Object primaryKey,String primaryKeyName){
        if(primaryKey.equals(page.getMaxValInPage())){
            Tuple tuple=page.getTuplesInPage().get(index-1);
            Object value = tuple.getPrimaryKey();
            page.setMaxValInPage(value);
        }else if(primaryKey.equals(page.getMinValInPage())){
            Tuple tuple=page.getTuplesInPage().get(index-1);
            Object value = tuple.getPrimaryKey();
            page.setMinValInPage(value);
        }
        page.getTuplesInPage().remove(index);
        if(page.getTuplesInPage().isEmpty()){
            return -1;
        }
        return 0;
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
        int countOfCols = 0;
        while(row!=null){
            arr = row.split(",");
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
        }
        if(countOfCols != htblColNameValue.size()){
            return false;
        }
        return foundTableName;
    }

    public String getPrimaryKeyName(String strTableName) throws IOException, DBAppException { //returns column name
        FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        String row = br.readLine();
        String[] arr = row.split(",");
        while(row!=null){
            arr = row.split(",");
            if(arr[0] == strTableName) {
                if(Boolean.parseBoolean(arr[3])){
                    return arr[1];
                }
            }
            row = br.readLine();
        }
        throw new DBAppException("No primary key found");
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


