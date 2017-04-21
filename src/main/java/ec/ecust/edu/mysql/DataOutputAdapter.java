package ec.ecust.edu.mysql;


import java.io.*;

/**
 * Created by pengfei on 2017/4/21.
 */
public class DataOutputAdapter  extends DataOutputStream {

    public DataOutputAdapter(OutputStream out) {
        super(out);
    }
}
