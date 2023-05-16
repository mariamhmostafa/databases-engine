package ML1;

public class DBAppException extends Exception {
    public DBAppException(){
        super();
    }
    public DBAppException(String s){
        super(s);
    }
    public DBAppException(Exception e) {
        super(e);
    }
}
