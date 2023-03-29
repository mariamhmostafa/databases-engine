package ML1;

import java.util.Hashtable;
import java.util.Vector;

public class Tuple {
    private Hashtable<String, Object> values;
    private Object primaryKey;

    public Tuple(Hashtable values, Object primaryKey){
        this.values = values;
        this.primaryKey = primaryKey;
    }

    public Object getPrimaryKey() {
        return primaryKey;
    }
}
