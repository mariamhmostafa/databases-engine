package ML1;

import java.io.Serializable;

public class NullWrapper implements Serializable {
    public NullWrapper(){
    }

    @Override
    public String toString() {
        return "null";
    }
}
