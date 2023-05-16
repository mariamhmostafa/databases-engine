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
        Hashtable<String,String> htblColNameType = new Hashtable<>();
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
        Hashtable<String,Object> htblColNameValue1 = new Hashtable<>();
        htblColNameValue1.put("id", 19);
        htblColNameValue1.put("name", "mariam");
        htblColNameValue1.put("gpa", 0.87);
        dbApp.insertIntoTable( strTableName , htblColNameValue1 );
        
        Hashtable<String,Object> htblColNameValue2 = new Hashtable<>();
        htblColNameValue2.put("id", 20);
        htblColNameValue2.put("name", "nairuzy");
        htblColNameValue2.put("gpa", 4.0);
        dbApp.insertIntoTable( strTableName , htblColNameValue2 );
        
        Hashtable<String,Object> htblColNameValue4 = new Hashtable<>();
        htblColNameValue4.put("id", 18);
        htblColNameValue4.put("name", "marwa");
        htblColNameValue4.put("gpa", 3.0);
        dbApp.insertIntoTable( strTableName , htblColNameValue4 );
        
        Hashtable<String,Object> htblColNameValue5 = new Hashtable<>( );
        htblColNameValue5.put("id", 17);
        htblColNameValue5.put("name", "sarah");
        htblColNameValue5.put("gpa", 3.0);
        dbApp.insertIntoTable( strTableName , htblColNameValue5 );
        
        Hashtable<String,Object> htblColNameValue6 = new Hashtable<>( );
        htblColNameValue6.put("id", 16);
        htblColNameValue6.put("name", "sarah");
        htblColNameValue6.put("gpa", 2.6);
        dbApp.insertIntoTable( strTableName , htblColNameValue6 );
        
        Hashtable<String,Object> htblColNameValue3 = new Hashtable<>( );
        htblColNameValue3.put("id", 26);
        htblColNameValue3.put("name", "frfr");
        htblColNameValue3.put("gpa", 2.6);
        dbApp.insertIntoTable( strTableName , htblColNameValue3 );
        
        String[] strarrColName = {"gpa", "name", "id"};
        dbApp.createIndex("student", strarrColName);
        Octree octree = (Octree) dbApp.deserializeObject("src/Resources/"+ strTableName+"0"+"Octree.ser");
        printOctree(octree);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[3];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[2] = new SQLTerm();
        String[]strarrOperators = new String[2];
        arrSQLTerms[0]._strTableName = "Student";
        arrSQLTerms[0]._strColumnName= "name";
        arrSQLTerms[0]._strOperator = ">=";
        arrSQLTerms[0]._objValue = "frfr";
//
        arrSQLTerms[1]._strTableName = "Student";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = new Double( 2.6 );

        strarrOperators[0] = "and";

        arrSQLTerms[2]._strTableName = "Student";
        arrSQLTerms[2]._strColumnName= "id";
        arrSQLTerms[2]._strOperator = ">";
        arrSQLTerms[2]._objValue = new Integer( 16 );
        strarrOperators[1] = "and";
//
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
        
//        Hashtable<String,Object> toUpdate = new Hashtable<>();
//        toUpdate.put("name", "mariam" );
//        toUpdate.put("gpa", 0.7);
//        dbApp.updateTable(strTableName,"19" ,toUpdate);
    
        while(resultSet.hasNext()) {
            Tuple t = (Tuple) resultSet.next();
            System.out.println();
            for (String key : t.getValues().keySet()) {
                System.out.println(key + " value: " + t.getValues().get(key).toString());
            }
        }
        
    }


    private static void printOctree(Octree octree){
        if(octree.isLeaf()) {
            for (Point p : octree.getPoints()) {
                System.out.print("x:" + p.getX() + " y:" + p.getY() + " z:" + p.getZ());
                System.out.println( p.getReference().toString());

            }
        } else{
            for(Octree bb: octree.getBbs()){
                printOctree(bb);
            }
        }
    }
    public void init() {
        FileWriter writer;
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
        BufferedReader br;
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
                boolean clusteringKey = colName.equals(strClusteringKeyColumn);
                sb.append(strTableName).append(", ");
                sb.append(colName).append(", ");
                sb.append(type).append(", ");
                sb.append(clusteringKey).append(", ");
                sb.append("null, ");
                sb.append("null, ");
                sb.append(min).append(", ");
                sb.append(max).append("\n");
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
            if(!(type.equalsIgnoreCase("java.lang.integer") || type.equalsIgnoreCase("java.lang.string") || type.equalsIgnoreCase("java.lang.double") || type.equalsIgnoreCase("java.lang.date")))
                return false;
            String min = htblColNameMin.get(colName);
            String max = htblColNameMax.get(colName);
            if(type.equalsIgnoreCase("java.lang.integer")){
                try{
                    Integer.parseInt(min);
                    Integer.parseInt(max);
                } catch(NumberFormatException | NullPointerException e) {
                    return false;
                }
            } else if(type.equalsIgnoreCase("java.lang.double")){
                try {
                    Double.parseDouble(min);
                    Double.parseDouble(max);
                } catch(NumberFormatException | NullPointerException e) {
                    return false;
                }
            } else if(type.equalsIgnoreCase("java.lang.date")){
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
                String Path=createPage(table, newtuple);
                insertIntoIndex(table,htblColNameValue,Path,newtuple.getPrimaryKey());
                serializeObject(table, "src/Resources/" + strTableName + ".ser");
                return;
            }
            int pathi = getPageIndex(table, newtuple);
            String pathName = table.getPaths().get(pathi);
            Page page = (Page) deserializeObject(pathName);
            int i = getNewIndex(page.getTuplesInPage(), value);
            page.getTuplesInPage().add(i, newtuple);
            insertIntoIndex(table,htblColNameValue,pathName,newtuple.getPrimaryKey());
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
                    insertIntoIndex(table,htblColNameValue,pathOfPage,lasttuple.getPrimaryKey());
                    serializeObject(table, "src/Resources/" + strTableName + ".ser");
                    return;
                }
                pathName = table.getPaths().get(++pathi);
                page = (Page) deserializeObject(pathName);
                i = getNewIndex(page.getTuplesInPage(), value);
                page.getTuplesInPage().add(i, lasttuple);
                insertIntoIndex(table,htblColNameValue,pathName,lasttuple.getPrimaryKey());
            }
            serializeObject(page, page.getPath());
            serializeObject(table, "src/Resources/" + strTableName + ".ser");

//     why   serializeObject(table, "src/Resources/" + strTableName + ".ser");
    }
    
    public void updateIndex(Table table,Tuple t,String path) throws DBAppException {
        HashSet<String> hs=new HashSet<>();
        for (String key:t.getValues().keySet()){
            String octreePath=table.getOctreePaths().get(key);
            if (octreePath!=null){
                if(!hs.contains(octreePath)){
                    hs.add(octreePath);
                    Octree tree=(Octree) deserializeObject(octreePath);
                    tree.updatePath((Comparable) t.getValues().get(tree.getColumns()[0]),(Comparable) t.getValues().get(tree.getColumns()[1]),(Comparable)t.getValues().get(tree.getColumns()[2]),t.getPrimaryKey(),path);
                    serializeObject(tree,octreePath);
                }
            }
        }
    }
    
    public void insertIntoIndex(Table table,Hashtable<String,Object> htblColNameValue,String path,Object clustringkey) throws DBAppException {
        HashSet<String> hs=new HashSet<>();
        for (String key:htblColNameValue.keySet()){
           String octreePath=table.getOctreePaths().get(key);
            if (octreePath!=null){
                if(!hs.contains(octreePath)){
                    hs.add(octreePath);
                    Octree tree=(Octree) deserializeObject(octreePath);
                    tree.insert((Comparable) htblColNameValue.get(tree.getColumns()[0]),(Comparable) htblColNameValue.get(tree.getColumns()[1]),(Comparable)htblColNameValue.get(tree.getColumns()[2]),path,clustringkey);
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
    
    public int findIndex(Vector<Tuple> tuples, Comparable value) {
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
        int mid;
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
                         if (table.getOctreePaths().get(primaryKeyName)!=null){ //use index
                             Octree octree = (Octree) deserializeObject(table.getOctreePaths().get(primaryKeyName));
                             serializeObject(table,"src/Resources/" + strTableName + ".ser");
                             updateUsingIndex(clusteringKeyValue,htblColNameValue,table);
                             return;
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
         }catch(IOException | ParseException e){
             throw new DBAppException(e);
         }
    }
    
    private void updateUsingIndex(Object clusteringKeyValue, Hashtable<String, Object> htblColNameValue,Table table) throws DBAppException{
        Hashtable <String, Object> toDelete = new Hashtable<>();
        toDelete.put(table.getStrClusteringKeyColumn(),clusteringKeyValue);
        ArrayList <Tuple> res = deleteUsingIndex(table,toDelete);
        Tuple oldTuple = res.get(0);
        oldTuple.getValues().putAll(htblColNameValue);
        insertIntoTable(table.strTableName,oldTuple.getValues());
    }
    
    public void updateInPage(Page page,int index,Hashtable<String,Object> htblColNameValue){
        for(String key: htblColNameValue.keySet()){
            Object value = htblColNameValue.get(key);
            page.getTuplesInPage().get(index).getValues().put(key,value);
        }
    }
    
    public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        strTableName = strTableName.toLowerCase();
        if(!someAreValid(strTableName,htblColNameValue)) throw new DBAppException("Wrong values");
        Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser"); //deserialize table to delete
        for(String key: htblColNameValue.keySet()){
            if(table.getOctreePaths().get(key) != null) {
                deleteUsingIndex(table, htblColNameValue);
                serializeObject(table, "src/Resources/" + strTableName + ".ser"); //serialize table after deleting as it has index
                return;
            }
        }
        String primaryKeyName = table.getStrClusteringKeyColumn();
        Object primaryKey = htblColNameValue.get(primaryKeyName);
        if (primaryKey != null) {
            deleteFromTable2(strTableName, htblColNameValue);
        } else {
            LinkedList<String> pagesToDelete = new LinkedList<>();
            for (String path : table.getPaths()) {
                Page page = (Page) deserializeObject(path);
                LinkedList<Tuple> toDelete = new LinkedList<>(); //use this tuple to delete from index
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
        serializeObject(table, "src/Resources/" + strTableName + ".ser"); //serialize table after deletion as there's no index used
        
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
    
    public ArrayList<Tuple> deleteUsingIndex(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        Table table = (Table) deserializeObject("src/Resources/" + strTableName + ".ser");
        //Vector<Octree> mayDelete = findUsingIndex(table, htblColNameValue);
        Hashtable<Object,String> mayDelete = findUsingIndex(table,htblColNameValue);
        ArrayList<Tuple> oldValues = new ArrayList<>();
        //object is vector of points
        for(Object key: mayDelete.keySet()){
            Vector<Point> points = (Vector<Point>)key;
            String path = mayDelete.get(key);
            Octree octree = (Octree) deserializeObject(path);
            for (int i=0; i<points.size(); i++) {
                Point point = points.get(i);
                Hashtable<Object, String> references = point.getReference();
                ArrayList<Object> toDelete = new ArrayList<>();
                for (Object clusteringKey : references.keySet()) {
                    System.out.println(clusteringKey);
                    String pagePath = references.get(clusteringKey);
                    Page page = (Page) deserializeObject(pagePath);
                    int index = findIndex(page.getTuplesInPage(), (Comparable) clusteringKey);
                    Tuple tuple = page.getTuplesInPage().get(index);
                    boolean willDelete = true;
                    for (String colName : htblColNameValue.keySet()) {
                        System.out.println("hm");
                        if (!htblColNameValue.get(colName.toLowerCase()).equals(tuple.getValues().get(colName.toLowerCase()))) {
                            willDelete = false;
                            break;
                        }
                    }
                    if (willDelete) {
                        oldValues.add(tuple);
                        page.getTuplesInPage().remove(tuple);
                        toDelete.add(clusteringKey);
                    }
                    serializeObject(page, pagePath);
                }
                for (Object k : toDelete) {
                    references.remove(k);
                }
                if (references.isEmpty()) {
                    octree.getPoints().remove(point);
                    System.out.println("ref empty");
                }
            }
            serializeObject(octree, path);
        }
        
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        return oldValues;
    }
    
    public Hashtable<Object, String> findUsingIndex(Table table , Hashtable<String,Object> htblColNameValue) throws DBAppException {
        Hashtable<Object, String> result= new Hashtable<>();
        //Vector<Octree> result = new Vector<>();
        HashSet<String> octrees = new HashSet<>();
        for(String key: htblColNameValue.keySet()){
            if(table.getOctreePaths().get(key) != null)
                octrees.add(table.getOctreePaths().get(key));
        }
        for(String path: octrees){
            Octree octree = (Octree) deserializeObject(path);
            Comparable x = null;
            Comparable y = null;
            Comparable z = null;
            if(htblColNameValue.containsKey(octree.getColumns()[0]))
                x = (Comparable) htblColNameValue.get(octree.getColumns()[0]);
            if(htblColNameValue.containsKey(octree.getColumns()[1]))
                y = (Comparable) htblColNameValue.get(octree.getColumns()[1]);
            if(htblColNameValue.containsKey(octree.getColumns()[2]))
                z = (Comparable) htblColNameValue.get(octree.getColumns()[2]);
            Hashtable<Object, String> recResult = recGetPos(octree, x, y, z, path);
            if(!recResult.isEmpty()){
                result.putAll(recResult);
            }
            serializeObject(octree,path);
        }
        return result;
    }
    public static Hashtable<Object,String> recGetPos(Octree octree,Comparable x, Comparable y, Comparable z,String path){
        if(octree.isLeaf()) {
            Hashtable<Object,String> htblResult = new Hashtable<>();
            Vector<Point> vec = octree.getPoints();
            if(vec != null){
                htblResult.put(vec, path);
            }
            return htblResult;
        }
        Comparable midx = octree.getMid(octree.getTopLeftFront().getX(), octree.getBottomRightBack().getX()); //gets median of every dimension
        Comparable midy = octree.getMid(octree.getTopLeftFront().getY(), octree.getBottomRightBack().getY());
        Comparable midz = octree.getMid(octree.getTopLeftFront().getZ(), octree.getBottomRightBack().getZ());
        HashSet<Integer> pos = getPos(x, y, z, midx, midy, midz);
        Hashtable<Object,String> res = new Hashtable<>();
        //Vector<Octree> res= new Vector<>();
        for(Integer t: pos){
            res.putAll(recGetPos(octree.getBbs()[t], x, y, z, octree.getBbsPaths()[t]));
        }
        return res;
    }
    
    public static HashSet<Integer> getPos(Comparable x, Comparable y, Comparable z, Comparable midx, Comparable midy, Comparable midz){
        HashSet<Integer> pos = new HashSet<>();
        pos.add(0); pos.add(1); pos.add(2); pos.add(3); pos.add(4); pos.add(5); pos.add(6); pos.add(7);
        if(x!=null){
            if(x.compareTo(midx)<=0){pos.remove(4); pos.remove(5); pos.remove(6); pos.remove(7);}
            else{pos.remove(0); pos.remove(1); pos.remove(2); pos.remove(3);}
        }
        if(y!=null){
            if(y.compareTo(midy)<=0){pos.remove(2); pos.remove(3); pos.remove(6); pos.remove(7);}
            else{pos.remove(0); pos.remove(1); pos.remove(4); pos.remove(5);}
        }
        if(z!=null){
            if(z.compareTo(midz)<=0){pos.remove(0); pos.remove(2); pos.remove(4); pos.remove(6);}
            else{pos.remove(1); pos.remove(3); pos.remove(5); pos.remove(7);}
        }
        return pos;
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
            String row;
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
        }catch(IOException | ParseException e){
            throw new DBAppException(e);
        }
    }
    
    public int[] shouldUseIndex(SQLTerm[] arrSQLTerms,int indx, String[] strarrOperators) throws DBAppException {
        System.out.println("InShouldUseIndex");
        int[] arr=new int[4];
        Arrays.fill(arr,-1);
        if(arrSQLTerms.length<3)return arr;

        Table table = (Table) deserializeObject("src/resources/" + arrSQLTerms[0]._strTableName + ".ser");
        for(int i=indx;i<arrSQLTerms.length;i++){
            int count=0;
            Arrays.fill(arr,-1);
            if(table.getOctreePaths().get(arrSQLTerms[i]._strColumnName)!=null){
                Octree tree=(Octree) deserializeObject(table.getOctreePaths().get(arrSQLTerms[i]._strColumnName));
                //-------------------------------------------------------------------------------
                String [] columns= tree.getColumns();

                for (int j=i;j<i+3;j++){
                    if (tree.getHtblColumns().get(arrSQLTerms[j]._strColumnName)!=null){
                         if(arr[tree.getHtblColumns().get(arrSQLTerms[j]._strColumnName)+1]==-1){
                           if(j!=i+2){
                               if(!strarrOperators[j].equalsIgnoreCase("AND")){
                                break;
                           }
                        }
                        arr[tree.getHtblColumns().get(arrSQLTerms[j]._strColumnName)+1]=j;
                           //System.out.println(arr[tree.getHtblColumns().get(arrSQLTerms[j]._strColumnName)+1]+" "+count);
                        count++;
                    }}
                    else {
                        break;
                    }
                }
                System.out.println(count);
                if (count==3){
                    arr[0]=i;
                    break;
                }
                serializeObject(tree,table.getOctreePaths().get(arrSQLTerms[i]._strColumnName));
            }
            else {
                System.out.println(table.getOctreePaths().toString());
            }
        }
        serializeObject(table, "src/Resources/" +arrSQLTerms[0]._strTableName  + ".ser");
        return arr;
    }
    public HashSet<Tuple> selectUsingIndex(SQLTerm[] arrSQLTerms, int[] arr) throws DBAppException {
        System.out.println("InSelectUsingIndex");
        Table table = (Table) deserializeObject("src/resources/" + arrSQLTerms[0]._strTableName + ".ser");
        Octree tree=(Octree) deserializeObject(table.getOctreePaths().get(arrSQLTerms[arr[1]]._strColumnName));
        HashSet<Tuple> tuples = new HashSet<>();
        Hashtable<Object,String> res=new Hashtable<>();
        tree.select(arrSQLTerms,arr,res);
        System.out.println(res.toString());
        for(Object primaryKey:res.keySet()){
            Page page = (Page) deserializeObject(res.get(primaryKey));
            for(Tuple tuple:page.getTuplesInPage()){
                if(((Comparable)tuple.getPrimaryKey()).compareTo(primaryKey)==0){
                    tuples.add(tuple);
                }
            }
            serializeObject(page,res.get(primaryKey));
        }
        System.out.println(tuples.size());
        serializeObject(tree,table.getOctreePaths().get(arrSQLTerms[arr[1]]._strColumnName));
        serializeObject(table, "src/Resources/" +arrSQLTerms[0]._strTableName  + ".ser");
         return tuples;
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException{
        validTerms(arrSQLTerms,strarrOperators);
        ArrayList<HashSet<Tuple>> arrOfArr = new ArrayList<>();
        int i=0;
        for( i=0;i<arrSQLTerms.length;i++){
           int[] arr=shouldUseIndex(arrSQLTerms,i,strarrOperators);
           if(arr[0]==-1){
               System.out.println(i+" "+arr[0]);
               break;
           }
           for(int j=i;j<arr[0];j++){
               System.out.println("elmafrod mated5olsh");
               arrOfArr.add(getSelectedTuples(arrSQLTerms[j]));
           }
            arrOfArr.add(selectUsingIndex(arrSQLTerms,arr));
            strarrOperators[arr[0]]="null";
            strarrOperators[arr[0]+1]="null";
            i=arr[0]+2;//to get the next unindexed sqlterms
        }
        while(i<arrSQLTerms.length){
            arrOfArr.add(getSelectedTuples(arrSQLTerms[i]));
            i++;
        }


        HashSet<Tuple> filtered = arrOfArr.get(0);
        for(int j=0; j<strarrOperators.length; j++){
            if(strarrOperators[j].equals("null")){
                continue;
            }
            String operator = strarrOperators[j];
            switch(operator.toLowerCase()){
                case "or":
                    filtered.addAll(arrOfArr.get(j+1));
                    break;
                case "and":
                    ArrayList<Tuple> remove=new ArrayList<>();
                    for(Tuple t : filtered){
                        if(!arrOfArr.get(j+1).contains(t)){
                            remove.add(t);
                        }
                    }
                    for(Tuple t:remove){
                        filtered.remove(t);
                    }
                    break;
                default:
                    for(Tuple t : arrOfArr.get(j+1)){
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
                return ((Comparable) value).compareTo(objValue) > 0;
            case ">=":
                return ((Comparable) value).compareTo(objValue) >= 0;
            case "<":
                return ((Comparable) value).compareTo(objValue) < 0;
            case "<=":
                return ((Comparable) value).compareTo(objValue) <= 0;
            case "!=":
                if(value instanceof NullWrapper && objValue == null){
                    return false;
                }
                if(value instanceof NullWrapper || objValue == null){
                    return true;
                }
                return ((Comparable) value).compareTo(objValue) != 0;
            default:
                if(value instanceof NullWrapper && objValue == null){
                    return true;
                }
                if(value instanceof NullWrapper || objValue == null){
                    return false;
                }
                return ((Comparable) value).compareTo(objValue) == 0;
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
        BufferedReader br;
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
                Date minDate;
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
                if(!(operator.equalsIgnoreCase("and") || operator.equalsIgnoreCase("or") || operator.equalsIgnoreCase("xor") )){
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
    private void isIndexValid(String strTableName, String[] strarrColName) throws DBAppException {
        if(strarrColName.length != 3){
            throw new DBAppException("not enough colunms");
        }

        try {
            FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            String row;
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
        for(String col:strarrColName){
            if(table.getOctreePaths().contains(col)){
                throw new DBAppException("Column already has an index");
            }
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
                octree.insert(x,y,z, page.getPath(),tuple.getPrimaryKey());
            }
            serializeObject(page, page.getPath());
        }
        String oPath = "src/Resources/" + strTableName + table.getOctreePaths().size()+"Octree.ser";
        for (String s : strarrColName){
            table.getOctreePaths().put(s, oPath);
        }
        serializeObject(table, "src/Resources/" + strTableName + ".ser");
        serializeObject(octree,oPath);
    }

    public void updateMetaFile(String tableName, String[] indexColumns) throws IOException {
        FileReader oldMetaDataFile = new FileReader("src/resources/metadata.csv");
        BufferedReader br = new BufferedReader(oldMetaDataFile);
        StringBuilder newMetaData = new StringBuilder();
        String curLine;
        StringBuilder indexName=new StringBuilder();
        for (String s:indexColumns){
            indexName.append(s);//just creating the indexName by appending all colNames
        }
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
                        if(i==4){
                            tmp.append(indexName).append(", ");}
                        else if(i==5){tmp.append("Octree, ");}
                        else if (i == 7)
                            tmp.append(curLineSplit[i]);
                        else
                            tmp.append(curLineSplit[i]).append(",");
                }
            }
            newMetaData.append(tmp).append("\n");
        }
        FileWriter metaDataFile = new FileWriter("src/resources/metadata.csv");
        metaDataFile.write(newMetaData.toString());
        metaDataFile.close();
    }

    private Comparable set(String type, String value) throws DBAppException {
        if (type.equalsIgnoreCase("java.lang.integer"))
            return Integer.parseInt(value);
        else if (type.equalsIgnoreCase("java.util.date")) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                return formatter.parse(value);
            } catch (ParseException e) {
                throw new DBAppException(e);
            }
        } else if (type.equalsIgnoreCase("java.lang.double"))
            return Double.parseDouble(value);
        return value;
    }

}


