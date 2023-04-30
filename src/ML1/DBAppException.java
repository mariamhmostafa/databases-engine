package ML1;

import java.io.IOException;

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
