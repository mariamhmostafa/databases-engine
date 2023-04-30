package ML1;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    
    public void init() {
        FileWriter writer = null;
        try {
            writer = new FileWriter( "src/Resources/metadata.csv", true );
            StringBuilder sb = new StringBuilder();
            sb.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max"+ "\n");
            writer.append(sb);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.out.println("FileWrite has problems;(");
        }

    }


    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException {
        BufferedReader br = null;
        strTableName = strTableName.toLowerCase();
        try {
            br = new BufferedReader(new FileReader("src/Resources/metadata.csv"));
            String line;
            while((line = br.readLine())!=null){
                String[] arr= line.split(", ");
                if(arr[0].equals(strTableName)) {
                    throw new DBAppException("Table name taken");
                }
            }
            if(!validCreation(strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax)){
                throw new DBAppException("Inconsistant Columns");
            }
            Table newTable = new Table(strTableName, strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
            serializeObject(newTable, "src/Resources/" + strTableName + ".ser");
            FileWriter writer = new  FileWriter  ( "src/Resources/metadata.csv", true );
            for(String colName : htblColNameType.keySet()){
                colName = colName.toLowerCase();
                StringBuilder sb = new StringBuilder();
                String type = htblColNameType.get(colName).toLowerCase();
                String min = htblColNameMin.get(colName).toLowerCase();
                String max = htblColNameMax.get(colName).toLowerCase();
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
        } catch (IOException e) {
            throw new DBAppException(e);
        }
    }

    public static boolean validCreation(String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                                        Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) {
        if(htblColNameType.size() != htblColNameMin.size() || htblColNameType.size() != htblColNameMax.size())
            return false;
        if(!htblColNameType.containsKey(strClusteringKeyColumn))
           return false;
        for(String colName : htblColNameType.keySet()) {
            if(!(htblColNameMin.containsKey(colName) && htblColNameMax.containsKey(colName)))
                return false;
            String type = htblColNameType.get(colName);
            if(!(type.toLowerCase().equals("java.lang.integer") || type.toLowerCase().equals("java.lang.string") || type.toLowerCase().equals("java.lang.double") || type.toLowerCase().equals("java.lang.date")))
                return false;
            String min = htblColNameMin.get(colName);
            String max = htblColNameMax.get(colName);
            if(type.toLowerCase().equals("java.lang.integer")){
                try{
                    Integer.parseInt(min);
                    Integer.parseInt(max);
                } catch(NumberFormatException e) {
                    return false;
                } catch(NullPointerException e) {
                    return false;
                }
            } else if(type.toLowerCase().equals("java.lang.double")){
                try {
                    Double.parseDouble(min);
                    Double.parseDouble(max);
                } catch(NumberFormatException e) {
                    return false;
                } catch(NullPointerException e) {
                    return false;
                }
            } else if(type.toLowerCase().equals("java.lang.date")){
                try {
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    formatter.parse(min);
                    formatter.parse(max);
                }   catch(ParseException e){
                    return false;
                }
            }
        }
        return true;
    }


    public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
   // checkIfContainsAllColumns(strTable)
            strTableName = strTableName.toLowerCase();
            if (!isValid(strTableName, htblColNameValue)) {
                throw new DBAppException("not valid :(");
            }
            Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
            String primaryKey = table.getStrClusteringKeyColumn();
            Comparable value = (Comparable) htblColNameValue.get(primaryKey);

            Tuple newtuple = new Tuple(htblColNameValue, value);
            if (table.getPaths().isEmpty()) {
                createPage(table, newtuple);
                serializeObject(table, "src/Resources/" + strTableName + ".ser");
                return;
            }
            int pathi = getPageIndex(table, newtuple);
            String pathName = table.getPaths().get(pathi);
            Page page = (Page) deserializeObject(pathName);
            int i = getNewIndex(page.getTuplesInPage(), value);
            page.getTuplesInPage().add(i, newtuple);
            if (page.getTuplesInPage().size() <= Integer.parseInt(Page.getVal("MaximumRowsCountinTablePage"))) {
                page.setMaxValInPage(page.getTuplesInPage().lastElement().getPrimaryKey());
                page.setMinValInPage(page.getTuplesInPage().firstElement().getPrimaryKey());
            }
            while (page.getTuplesInPage().size() > Integer.parseInt(Page.getVal("MaximumRowsCountinTablePage"))) {
                Tuple lasttuple = page.getTuplesInPage().lastElement();
                serializeObject(page, page.getPath());
                value = (Comparable) lasttuple.getValues().get(primaryKey);
                deleteFromTable(strTableName, lasttuple.getValues());
                if (pathi == table.getPaths().size() - 1) {
                    createPage(table, lasttuple);
                    serializeObject(table, "src/Resources/" + strTableName + ".ser");
                    return;
                }
                pathName = table.getPaths().get(++pathi);
                page = (Page) deserializeObject(pathName);
                i = getNewIndex(page.getTuplesInPage(), value);
                page.getTuplesInPage().add(i, newtuple);
            }
            serializeObject(page, page.getPath());
            serializeObject(table, "src/Resources/" + strTableName + ".ser");

//     why   serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }

    public void createPage(Table table, Tuple newtuple) throws DBAppException{
        Page page = new Page("src/Resources/"+table.getStrTableName()+table.getPageCounter()+".ser");
        table.setPageCounter(table.getPageCounter()+1);
        page.setMinValInPage(newtuple.getPrimaryKey());
        page.setMaxValInPage(newtuple.getPrimaryKey());
        page.getTuplesInPage().add(newtuple);
        table.getPaths().add(page.getPath());
        serializeObject(page, page.getPath());

    }
    
    public int getPageIndex(Table table, Tuple tuple) throws DBAppException {

            int pageIndex = 0;
            for (int i = 1; i < table.getPaths().size(); i++) {
                Page page = (Page) deserializeObject(table.getPaths().get(i));
                if (((Comparable) page.getMinValInPage()).compareTo((Comparable) (tuple.getPrimaryKey())) < 0) {
                    pageIndex = i;
                }
                serializeObject(page, table.getPaths().get(i));
            }
            return pageIndex;

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

    public int getNewIndex(Vector<Tuple> tuples, Comparable value) throws DBAppException {
        int low = 0;
        int high = tuples.size()-1;
        int mid=0;
        while(low<=high){
            mid = ((high-low)/2) + low;
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
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException {
        strTableName = strTableName.toLowerCase();
         if(!someAreValid(strTableName,htblColNameValue)) throw new DBAppException("Wrong values!");
         try {
             Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
             String primaryKeyName = table.getStrClusteringKeyColumn();
             if (htblColNameValue.containsKey(primaryKeyName)) {
                 throw new DBAppException("Cannot Update primary key");
             }
             if (strClusteringKeyValue.equals("")) {
                 throw new DBAppException("Primary key cannot be empty");
             }
             FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
             BufferedReader br = new BufferedReader(oldMetaDataFile);
             String row;
             String[] arr;
             Object clusteringKeyValue = null;
             while ((row = br.readLine()) != null) {
                 arr = row.split(", ");
                 if (arr[0].equals(strTableName)) {
                     String colName = arr[1];
                     String colType = arr[2].toLowerCase();
                     if (colName.equalsIgnoreCase(primaryKeyName)) {
                         if (colType.equals("java.lang.integer")) {
                             clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
                         } else if (colType.equals("java.lang.string")) {
                             clusteringKeyValue = strClusteringKeyValue;
                         } else if (colType.equals("java.util.date")) {
                             SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                             clusteringKeyValue = formatter.parse(strClusteringKeyValue);
                         } else if (colType.equals("java.lang.double")) {
                             clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
                         }
                         int indexInPage = -1;
                         for (String pathName : table.getPaths()) {
                             Page page = (Page) deserializeObject(pathName);
                             Object max = page.getTuplesInPage().get(page.getTuplesInPage().size() - 1).getPrimaryKey();
                             Object min = page.getTuplesInPage().get(0).getPrimaryKey();
                             if (clusteringKeyValue instanceof String) {
                                 if (((String) max).compareTo((String) clusteringKeyValue) >= 0 && ((String) min).compareTo((String) clusteringKeyValue) <= 0) {
                                     indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                                 }
                             } else if (clusteringKeyValue instanceof Integer) {
                                 if (((Integer) max).compareTo((Integer) clusteringKeyValue) >= 0 && ((Integer) min).compareTo((Integer) clusteringKeyValue) <= 0) {
                                     indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                                 }
                             } else if (clusteringKeyValue instanceof Double) {
                                 if (((Double) max).compareTo((Double) clusteringKeyValue) >= 0 && ((Double) min).compareTo((Double) clusteringKeyValue) <= 0) {
                                     indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                                 }
                             } else {
                                 if (((Date) max).compareTo((Date) clusteringKeyValue) >= 0 && ((Date) min).compareTo((Date) clusteringKeyValue) <= 0) {
                                     indexInPage = findIndex(page.getTuplesInPage(), (Comparable) clusteringKeyValue);
                                 }
                             }
                             if (indexInPage != -1) { //may update a different tuple if clustering key not found
                                 updateInPage(page, indexInPage, htblColNameValue);
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
         }catch(IOException e){
             throw new DBAppException(e);
         }catch(ParseException e){
             throw new DBAppException(e);
         }
     }
     
    public void updateInPage(Page page,int index,Hashtable<String,Object> htblColNameValue){
        for(String key: htblColNameValue.keySet()){
            Object value = htblColNameValue.get(key);
            page.getTuplesInPage().get(index).getValues().put(key,value);
        }
    }
    
    public void deleteFromTable2(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {

            Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
            String primaryKeyName = table.getStrClusteringKeyColumn();
            Object primaryKey = htblColNameValue.get(primaryKeyName);
            int indexInPage = -1;
            for (String pathName : table.getPaths()) {
                Page page = (Page) deserializeObject(pathName);
                Object max = page.getTuplesInPage().get(page.getTuplesInPage().size() - 1).getPrimaryKey();
                Object min = page.getTuplesInPage().get(0).getPrimaryKey();
                if (primaryKey instanceof String) {
                    if (((String) max).compareTo((String) primaryKey) >= 0 && ((String) min).compareTo((String) primaryKey) <= 0) {
                        indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                    }
                } else if (primaryKey instanceof Integer) {
                    if (((Integer) max).compareTo((Integer) primaryKey) >= 0 && ((Integer) min).compareTo((Integer) primaryKey) <= 0) {
                        indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                    }
                } else if (primaryKey instanceof Double) {
                    if (((Double) max).compareTo((Double) primaryKey) >= 0 && ((Double) min).compareTo((Double) primaryKey) <= 0) {
                        indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                    }
                } else {
                    if (((Date) max).compareTo((Date) primaryKey) >= 0 && ((Date) min).compareTo((Date) primaryKey) <= 0) {
                        indexInPage = findIndex(page.getTuplesInPage(), (Comparable) primaryKey);
                    }
                }
                if (indexInPage != -1) {
                    page.getTuplesInPage().remove(indexInPage);
                    page.setMaxValInPage(page.getTuplesInPage().lastElement().getPrimaryKey());
                    page.setMinValInPage(page.getTuplesInPage().firstElement().getPrimaryKey());
//                deleteFromPage(page, indexInPage, primaryKey, primaryKeyName);
                    if (page.getTuplesInPage().isEmpty()) {
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

    public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        strTableName = strTableName.toLowerCase();
        if(!someAreValid(strTableName,htblColNameValue)) throw new DBAppException("Wrong values");
        Table table = null;
            table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
            String primaryKeyName = table.getStrClusteringKeyColumn();
            Object primaryKey = htblColNameValue.get(primaryKeyName);
            if (primaryKey != null) {
                deleteFromTable2(strTableName, htblColNameValue);
            } else {
                LinkedList<String> pagesToDelete = new LinkedList<>();
                for (String path : table.getPaths()) {
                    Page page = (Page) deserializeObject(path);
                    LinkedList<Tuple> toDelete = new LinkedList<>();
                    for (Tuple record : page.getTuplesInPage()) {
                        boolean allConditionsMet = true;
                        for (String key : htblColNameValue.keySet()) {
                            if (!(record.getValues().get(key).equals(htblColNameValue.get(key)))) {
                                allConditionsMet = false;
                                break;
                            }
                        }
                        if (allConditionsMet) {
                            toDelete.add(record);
                        }
                    }
                    while (!toDelete.isEmpty()) {
                        page.getTuplesInPage().remove(toDelete.remove());
                    }
                    if (page.getTuplesInPage().isEmpty()) {
                        pagesToDelete.add(path);
                    } else {
                        page.setMaxValInPage(page.getTuplesInPage().lastElement().getPrimaryKey());
                        page.setMinValInPage(page.getTuplesInPage().firstElement().getPrimaryKey());
                        serializeObject(page, page.getPath());
                    }
                }
                while (!pagesToDelete.isEmpty()) {
                    deletePage(table, pagesToDelete.remove());
                }
            }
            serializeObject(table, "src/Resources/" + strTableName + ".ser");

    }
    
    public void deletePage(Table table, String pathName) throws DBAppException{
        table.getPaths().remove(pathName);
        Path path= Paths.get(pathName);
        try {
            Files.deleteIfExists(path);
        }catch(IOException e) {
            throw new DBAppException(e);
        }
    }

    public boolean isValid(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        try {
            FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            String row = "";
            String[] arr;
            boolean foundTableName = false;
            int counterInsert = 0;
            strTableName = strTableName.toLowerCase();
            while ((row = br.readLine()) != null) {
                arr = row.split(", ");
                if (arr[0].equals(strTableName)) {
                    foundTableName = true;
                    String colName = arr[1];
                    String colType = arr[2].toLowerCase();
                    String min = arr[6];
                    String max = arr[7];
                    Object object = htblColNameValue.get(colName);
                    Table table = (Table) deserializeObject("src/resources/" + strTableName + ".ser");
                    String primaryKeyName = table.getStrClusteringKeyColumn().toLowerCase();
                    Object primaryKey = htblColNameValue.get(primaryKeyName);
                    if (primaryKey == null) {
                        throw new DBAppException("Primary key cannot be null");
                    }
                    if (object == null) {
                        htblColNameValue.put(colName, new NullWrapper());
                        continue;
                    } else if (colType.equals("java.lang.integer")) {
                        if (!(object instanceof java.lang.Integer)) {
                            return false;
                        }
                        int minI = Integer.parseInt(min);
                        int maxI = Integer.parseInt(max);
                        if (((Integer) object) < minI || ((Integer) object) > maxI) {
                            return false;
                        }
                        counterInsert++;
                    } else if (colType.equals("java.lang.string")) {
                        if (!(object instanceof java.lang.String)) {
                            return false;
                        }
                        if (((String) object).compareTo(min) < 0 || ((String) object).compareTo(max) > 0) {
                            return false;
                        }
                        counterInsert++;
                    } else if (colType.equals("java.util.date")) {
                        if (!(object instanceof java.util.Date)) {
                            return false;
                        }
                        try {
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            Date minDate = formatter.parse(min);
                            Date maxDate = formatter.parse(max);
                            if (((Date) object).compareTo(minDate) < 0 || ((Date) object).compareTo(maxDate) > 0) {
                                return false;
                            }
                        } catch(ParseException e){
                            throw new DBAppException(e);
                        }
                        counterInsert++;
                    } else if (colType.equals("java.lang.double")) {
                        if (!(object instanceof java.lang.Double)) {
                            return false;
                        }
                        double minD = Double.parseDouble(min);
                        double maxD = Double.parseDouble(max);
                        if (((Double) object) < minD || ((Double) object) > maxD) {
                            return false;
                        }
                        counterInsert++;
                    }
                }
            }
            if (!(counterInsert == htblColNameValue.size())) throw new DBAppException("columns not found");
            return foundTableName;
        } catch(IOException e){
            throw new DBAppException(e);
        }
    }
    
    public boolean someAreValid(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        try {
            FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            String row;
            String[] arr;
            int counterUpdate = 0;
            boolean foundTableName = false;
            while ((row = br.readLine()) != null) {
                arr = row.split(", ");
                if (arr[0].equals(strTableName)) {
                    foundTableName = true;
                    String colName = arr[1];
                    String colType = arr[2].toLowerCase();
                    String min = arr[6];
                    String max = arr[7];
                    Object object = htblColNameValue.get(colName);
                    if (object == null) {
                        continue;
                    }
                    if (colType.equals("java.lang.integer")) {
                        if (!(object instanceof java.lang.Integer)) {
                            return false;
                        }
                        int minI = Integer.parseInt(min);
                        int maxI = Integer.parseInt(max);
                        if (((Integer) object) < minI || ((Integer) object) > maxI) {
                            return false;
                        }
                        counterUpdate++;
                    } else if (colType.equals("java.lang.string")) {
                        if (!(object instanceof java.lang.String)) {
                            return false;
                        }
                        if (((String) object).compareTo(min) < 0 || ((String) object).compareTo(max) > 0) {
                            return false;
                        }
                        counterUpdate++;
                    } else if (colType.equals("java.util.date")) {
                        if (!(object instanceof java.util.Date)) {
                            return false;
                        }
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        Date minDate = formatter.parse(min);
                        Date maxDate = formatter.parse(max);
                        if (((Date) object).compareTo(minDate) < 0 || ((Date) object).compareTo(maxDate) > 0) {
                            return false;
                        }
                        counterUpdate++;
                    } else if (colType.equals("java.lang.double")) {
                        if (!(object instanceof java.lang.Double)) {
                            return false;
                        }
                        double minD = Double.parseDouble(min);
                        double maxD = Double.parseDouble(max);
                        if (((Double) object) < minD || ((Double) object) > maxD) {
                            return false;
                        }
                        counterUpdate++;
                    }
                }
            }
            if (!(counterUpdate == htblColNameValue.size())) throw new DBAppException("columns not found");
            return foundTableName;
        }catch(IOException e){
            throw new DBAppException(e);
        }catch(ParseException e){
            throw new DBAppException(e);
        }
    }
    
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException{
        return null;
    }
    
    protected Object deserializeObject(String path) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Object o = objectIn.readObject();
            objectIn.close();
            fileIn.close();
            return o;
        }catch(IOException e){
            throw new DBAppException(e);
        }catch(ClassNotFoundException e){
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


