package ec.ecust.edu;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by pengfei on 2017/4/13.
 */
public class DisplayClusterTest {

    private String clusterPath = "hdfs://localhost:8020/features/result/canopies" ;

    @Test
    public void testLoadClustersWritable() throws IOException {
    	System.out.println("123") ; 
        DisplayCluster.loadClustersWritable(new Path(clusterPath)) ;
    }

}






