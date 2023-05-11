package ML1;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {
    
    public static void main(String[] args) throws DBAppException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        
        String strTableName = "student";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        Hashtable <String, String> colMin = new Hashtable<>();
        colMin.put("id", "0");
        colMin.put("name", "a");
        colMin.put("gpa", "0.0");
        Hashtable <String, String> colMax = new Hashtable<>();
        colMax.put("id", "100");
        colMax.put("name", "zzzzzzzzzzz");
        colMax.put("gpa","5.0");
        dbApp.createTable( strTableName, "id", htblColNameType, colMin, colMax);
        
        //////inserting into table:
        Hashtable htblColNameValue1 = new Hashtable( );
        htblColNameValue1.put("id", 19);
        htblColNameValue1.put("name", new String("mariam" ) );
        htblColNameValue1.put("gpa", new Double( 0.87) );
        dbApp.insertIntoTable( strTableName , htblColNameValue1 );
        Hashtable htblColNameValue2 = new Hashtable( );
        htblColNameValue2.put("id", new Integer( 20 ));
        htblColNameValue2.put("name", new String("nairuzy" ) );
        htblColNameValue2.put("gpa", new Double( 4.0) );
        dbApp.insertIntoTable( strTableName , htblColNameValue2 );
        
        Hashtable htblColNameValue4 = new Hashtable( );
        htblColNameValue4.put("id", new Integer( 18));
        htblColNameValue4.put("name", new String("marwa" ) );
        htblColNameValue4.put("gpa", new Double( 3.0) );
        dbApp.insertIntoTable( strTableName , htblColNameValue4 );
        Hashtable htblColNameValue5 = new Hashtable( );
        htblColNameValue5.put("id", new Integer( 17));
        htblColNameValue5.put("name", new String("sarah" ) );
        htblColNameValue5.put("gpa", new Double( 3.0) );
        dbApp.insertIntoTable( strTableName , htblColNameValue5 );
        Hashtable htblColNameValue6 = new Hashtable( );
        htblColNameValue6.put("id", new Integer( 16));
        htblColNameValue6.put("name", new String("sarah" ) );
        htblColNameValue6.put("gpa", new Double( 2.6) );
        dbApp.insertIntoTable( strTableName , htblColNameValue6 );
        Hashtable htblColNameValue3 = new Hashtable( );
        htblColNameValue3.put("id", new Integer( 26));
        htblColNameValue3.put("name", new String("frfr" ) );
        htblColNameValue3.put("gpa", new Double( 2.6) );
        dbApp.insertIntoTable( strTableName , htblColNameValue3 );
        String[] strarrColName = {"gpa", "name", "id"};
        dbApp.createIndex("student", strarrColName);
//        Octree octree = (Octree) dbApp.deserializeObject("src/Resources/"+ strTableName+1+"Octree.ser");
//        printOctree(octree);
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[2];
//        arrSQLTerms[0] = new SQLTerm();
//        arrSQLTerms[1] = new SQLTerm();
//        arrSQLTerms[0]._strTableName = "Student";
//        arrSQLTerms[0]._strColumnName= "name";
//        arrSQLTerms[0]._strOperator = ">=";
//        arrSQLTerms[0]._objValue = null;
//
//        arrSQLTerms[1]._strTableName = "Student";
//        arrSQLTerms[1]._strColumnName= "gpa";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue = new Double( 2.6 );
//        String[]strarrOperators = new String[1];
//        strarrOperators[0] = "OR";
//
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
        
//        Hashtable toUpdate = new Hashtable();
//        toUpdate.put("name", "mariam" );
//        toUpdate.put("gpa", 0.7);
//        dbApp.updateTable(strTableName,"19" ,toUpdate);
    
//        while(resultSet.hasNext()) {
//            Tuple t = (Tuple) resultSet.next();
//            System.out.println();
//            for (String key : t.getValues().keySet()) {
//                System.out.println(key + " value: " + t.getValues().get(key).toString());
//            }
//        }
        
    }


    private static void printOctree(Octree octree){
        if(octree.isLeaf()) {
            for (Point p : octree.getPoints()) {
                System.out.print("x:" + p.getX() + " y:" + p.getY() + " z:" + p.getZ());
                for (String page : p.getPagePath()) {
                    System.out.print(" page:" + page);
                }
                System.out.println();
            }
        } else{
            for(Octree bb: octree.getBbs()){
                printOctree(bb);
            }
        }
    }
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
                throw new DBAppException("Inconsistent Columns");
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
                    String pathOfPage=createPage(table, lasttuple);
                    insertIntoIndex(table,htblColNameValue,pathOfPage);
                    serializeObject(table, "src/Resources/" + strTableName + ".ser");
                    return;
                }
                pathName = table.getPaths().get(++pathi);
                page = (Page) deserializeObject(pathName);
                i = getNewIndex(page.getTuplesInPage(), value);
                page.getTuplesInPage().add(i, newtuple);
                insertIntoIndex(table,htblColNameValue,pathName);
            }
            serializeObject(page, page.getPath());
            serializeObject(table, "src/Resources/" + strTableName + ".ser");

//     why   serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }

    public void insertIntoIndex(Table table,Hashtable<String,Object> htblColNameValue,String path) throws DBAppException {
        HashSet<String> hs=new HashSet<>();

        for (String key:htblColNameValue.keySet()){
           String octreePath=table.getOctreePaths().get(key);
            if (octreePath!=null){
                if(!hs.contains(octreePath)){
                    hs.add(octreePath);
                    Octree tree=(Octree) deserializeObject(octreePath);
                    tree.insert((Comparable) htblColNameValue.get(tree.getColumns()[0]),(Comparable) htblColNameValue.get(tree.getColumns()[1]),(Comparable)htblColNameValue.get(tree.getColumns()[2]),path);
                    serializeObject(tree,octreePath);
            }}
        }

    }


    public String createPage(Table table, Tuple newtuple) throws DBAppException{
        Page page = new Page("src/Resources/"+table.getStrTableName()+table.getPageCounter()+".ser");
        table.setPageCounter(table.getPageCounter()+1);
        page.setMinValInPage(newtuple.getPrimaryKey());
        page.setMaxValInPage(newtuple.getPrimaryKey());
        page.getTuplesInPage().add(newtuple);
        table.getPaths().add(page.getPath());
        serializeObject(page, page.getPath());
        return page.getPath();
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
                        counterInsert++;
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
        validTerms(arrSQLTerms,strarrOperators);
        ArrayList<HashSet<Tuple>> arrOfArr = new ArrayList<>();
        for(SQLTerm sqlTerm : arrSQLTerms){
            arrOfArr.add(getSelectedTuples(sqlTerm));
        }
        HashSet<Tuple> filtered = arrOfArr.get(0);
        for(int i=0; i<strarrOperators.length; i++){
            String operator = strarrOperators[i];
            switch(operator.toLowerCase()){
                case "or":
                    filtered.addAll(arrOfArr.get(i+1));
                    break;
                case "and":
                    for(Tuple t : arrOfArr.get(i+1)){
                        if(!filtered.contains(t)){
                            filtered.remove(t);
                        }
                    }
                    break;
                default:
                    for(Tuple t : arrOfArr.get(i+1)){
                        if(filtered.contains(t)) {
                            filtered.remove(t);
                        }else{
                            filtered.add(t);
                        }
                    }
            }
        }
        return filtered.iterator();
    }
    
    public HashSet<Tuple> getSelectedTuples(SQLTerm sqlTerm) throws DBAppException {
        HashSet<Tuple> tuples = new HashSet<>();
        Table table = (Table) deserializeObject("src/resources/" + sqlTerm._strTableName + ".ser");

        for(String path : table.getPaths()){
            Page page = (Page) deserializeObject(path);
            for(Tuple tuple : page.getTuplesInPage()){
                try {
                    if (compareValues(sqlTerm, tuple)) {
                        tuples.add(tuple);
                    }
                }catch (Exception e){
                    throw new DBAppException(e.getMessage());
                }
            }
            serializeObject(page, path);
        }
        return tuples;
    }
    
    public boolean compareValues(SQLTerm sqlTerm, Tuple tuple){
        Object value = tuple.getValues().get(sqlTerm._strColumnName);
        Object objValue = sqlTerm._objValue;
        switch(sqlTerm._strOperator){
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
    
    
    public void validTerms(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if(strarrOperators.length != arrSQLTerms.length-1){
            throw new DBAppException("Size not consistent");
        }
        String tableName = arrSQLTerms[0]._strTableName.toLowerCase();
        for(SQLTerm term : arrSQLTerms){
            if(!term._strTableName.toLowerCase().equals(tableName)){
                throw new DBAppException("table names not consistent");
            }
        }
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/Resources/metadata.csv"));
            String line;
            boolean tableFound = false;
            while((line = br.readLine())!=null){
                String[] arr= line.split(", ");
                if(arr[0].equals(tableName)) {
                    tableFound = true;
                }
            }
            if(!tableFound){
                throw new DBAppException("table not found");
            }
        }  catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
        
        Table table = (Table) deserializeObject("src/resources/" + tableName + ".ser");
        Hashtable<String, String> htblColNameType = table.getHtblColNameType();
        Hashtable<String, String> htblColNameMin = table.getHtblColNameMin();
        Hashtable<String, String> htblColNameMax = table.getHtblColNameMax();
        for(SQLTerm term : arrSQLTerms){
            if(!htblColNameType.containsKey(term._strColumnName.toLowerCase())){
                throw new DBAppException("Column name not found");
            }
            if(!(term._strOperator.equals(">") || term._strOperator.equals(">=") || term._strOperator.equals("<") ||
                    term._strOperator.equals("<=") || term._strOperator.equals("!=") || term._strOperator.equals("="))){
                throw new DBAppException("Invalid Operator");
            }
            String colType=htblColNameType.get(term._strColumnName.toLowerCase());
            Object object = term._objValue;
            String min = htblColNameMin.get(term._strColumnName);
            String max = htblColNameMax.get(term._strColumnName);
            if (colType.equals("java.lang.integer")) {
                if (!(object instanceof java.lang.Integer)) {
                    throw new DBAppException("Type incorrect");
                }
                int minI = Integer.parseInt(min);
                int maxI = Integer.parseInt(max);
                if (((Integer) object) < minI || ((Integer) object) > maxI) {
                    throw new DBAppException("Out of range");
                }
            } else if (colType.equals("java.lang.string")) {
                if (!(object instanceof java.lang.String)) {
                    throw new DBAppException("Type incorrect");
                }
                if (((String) object).compareTo(min) < 0 || ((String) object).compareTo(max) > 0) {
                    throw new DBAppException("Out of range");
                }
            } else if (colType.equals("java.util.date")) {
                if (!(object instanceof java.util.Date)) {
                    throw new DBAppException("Type incorrect");
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                Date minDate = null;
                try {
                    minDate = formatter.parse(min);
                    Date maxDate = formatter.parse(max);
                    if (((Date) object).compareTo(minDate) < 0 || ((Date) object).compareTo(maxDate) > 0) {
                        throw new DBAppException("Out of range");
                    }
                } catch (ParseException e) {
                    throw new DBAppException(e.getMessage());
                }
            } else if (colType.equals("java.lang.double")) {
                if (!(object instanceof java.lang.Double)) {
                    throw new DBAppException("Type incorrect");
                }
                double minD = Double.parseDouble(min);
                double maxD = Double.parseDouble(max);
                if (((Double) object) < minD || ((Double) object) > maxD) {
                    throw new DBAppException("Out of range");
                }
            }
            for(String operator : strarrOperators){
                if(!(operator.toLowerCase().equals("and") || operator.toLowerCase().equals("or") || operator.toLowerCase().equals("xor") )){
                    throw new DBAppException("Invalid Operator");
                }
            }
        }
        
        serializeObject(table , "src/resources/" + tableName + ".ser");
        
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
    private void isIndexValid(String strTableName, String[] strarrColName) throws DBAppException {
        if(strarrColName.length != 3){
            throw new DBAppException("not enough colunms");
        }

        try {
            FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            String row = "";
            String[] arr;
            boolean foundTableName = false;
            strTableName = strTableName.toLowerCase();
            while ((row = br.readLine()) != null) {
                arr = row.split(", ");
                if (arr[0].equals(strTableName))
                    foundTableName = true;
            }
            if(!foundTableName)
                throw new DBAppException("Table not found");
        }catch(IOException e){
            throw new DBAppException(e);
        }
        Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
        for(String col: strarrColName){
            if(!table.htblColNameType.containsKey(col.toLowerCase()))
                throw new DBAppException("Column name not found");
        }
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }
    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{
       isIndexValid(strTableName, strarrColName);
        Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
        Comparable minx,miny,minz,maxx,maxy,maxz;
        minx = set(table.getHtblColNameType().get(strarrColName[0].toLowerCase()), table.getHtblColNameMin().get(strarrColName[0].toLowerCase()));
        maxx = set(table.getHtblColNameType().get(strarrColName[0].toLowerCase()), table.getHtblColNameMax().get(strarrColName[0].toLowerCase()));
        miny = set(table.getHtblColNameType().get(strarrColName[1].toLowerCase()), table.getHtblColNameMin().get(strarrColName[1].toLowerCase()));
        maxy = set(table.getHtblColNameType().get(strarrColName[1].toLowerCase()), table.getHtblColNameMax().get(strarrColName[1].toLowerCase()));
        minz = set(table.getHtblColNameType().get(strarrColName[2].toLowerCase()), table.getHtblColNameMin().get(strarrColName[2].toLowerCase()));
        maxz = set(table.getHtblColNameType().get(strarrColName[2].toLowerCase()), table.getHtblColNameMax().get(strarrColName[2].toLowerCase()));
        Octree octree=new Octree(minx,miny,minz,maxx,maxy,maxz,strarrColName[0].toLowerCase(),strarrColName[1].toLowerCase(),strarrColName[2].toLowerCase());
        for(String path: table.getPaths()){
            Page page = (Page) deserializeObject(path);
            for(Tuple tuple: page.getTuplesInPage()) {
                Comparable x  = (Comparable) tuple.getValues().get(strarrColName[0]);
                Comparable y  = (Comparable) tuple.getValues().get(strarrColName[1]);
                Comparable z  = (Comparable) tuple.getValues().get(strarrColName[2]);
                octree.insert(x,y,z, page.getPath());
            }
            serializeObject(page, page.getPath());
        }
        String oPath = "src/Resources/" + strTableName + table.getOctreePaths().size()+"Octree.ser";
        for(int i=0; i<strarrColName.length; i++)
            table.getOctreePaths().put(strarrColName[i], oPath);
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        serializeObject(octree,oPath);
    }



    public void updateMetaFile(String tableName, String[] indexColumns) throws IOException {

        FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);

        StringBuilder newMetaData = new StringBuilder();
        String curLine = "";

        StringBuilder indexName=new StringBuilder();
        for (String s:indexColumns){indexName.append(s);} //just creating the indexName by appending all colNames
        while ((curLine = br.readLine()) != null) {
            String[] curLineSplit = curLine.split(",");

            if (!curLineSplit[0].equals(tableName)) {
                newMetaData.append(curLine);
                newMetaData.append("\n");
                continue;
            }
            StringBuilder tmp = new StringBuilder(curLine);

            for (String col : indexColumns) {
                if (col.equals(curLineSplit[1])) {
                    tmp = new StringBuilder();
                    for (int i = 0; i < curLineSplit.length; i++)
                        if(i==4){tmp.append(indexName+", ");}
                        else if(i==5){tmp.append("Octree, ");}
                        else if (i == 7)
                            tmp.append(curLineSplit[i]);
                        else
                            tmp.append(curLineSplit[i] + ",");
                }
            }
            newMetaData.append(tmp + "\n");
        }

        FileWriter metaDataFile = new FileWriter("src/resources/metadata.csv");
        metaDataFile.write(newMetaData.toString());
        metaDataFile.close();

    }



    private Comparable set(String type, String value) throws DBAppException {
        if (type.toLowerCase().equals("java.lang.integer"))
            return Integer.parseInt(value);
        else if (type.toLowerCase().equals("java.util.date")) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                return formatter.parse(value);
            } catch (ParseException e) {
                throw new DBAppException(e);
            }
        } else if (type.toLowerCase().equals("java.lang.double"))
            return Double.parseDouble(value);
        return value;
    }

}


