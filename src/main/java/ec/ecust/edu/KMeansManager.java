package ec.ecust.edu;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;

/**
 * Created by pengfei on 2017/4/13.
 */
public class KMeansManager {

    private KmConf kmConf ;
    private Solver solver ;
    public KMeansManager(KmConf kmConf , Solver solver){
        this.kmConf = kmConf ;
        this.solver = solver ;
    }
    public void run(){
        Path pathIn = getClusterIn(kmConf.getInpath())  ;
//        KMeansDriver.run();
    }

    private Path getClusterIn(String inpath) {
        return null ;
    }
}




















