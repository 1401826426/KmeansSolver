package mahout;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

/**
 * Created by pengfei on 2017/4/11.
 */
public class FuzzyKmeansTest {

    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
    private static final String HDFS = "hdfs://localhost:8020";
//    private static final String localFile = "src/main/resources/123.txt";
//    private static String inPath = HDFS + "/kmeans/mix_data";
    private static String inPath = HDFS + "/features";
//    private static String seqFilePath = inPath+"/seqfile";
    private static String outPath = inPath+"/result/";
//    private Logger log = LoggerFactory.getLogger(this.getClass()) ;
    public void runJob(Path input , Path output , double t1 , double t2 , DistanceMeasure measure ,
                       double convergenceDelta ,int maxIterations , float fuzziness) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
//        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
//        log.info("Running Canopy to get initial clusters");
        Path canopyOutput = new Path(output, "canopies");
//        CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, canopyOutput, measure, t1, t2, false, 0.0, false);
//        log.info("Running FuzzyKMeans");
//        FuzzyKMeansDriver.run(directoryContainingConvertedInput, new Path(canopyOutput, "clusters-0-final"), output,
//                convergenceDelta, maxIterations, fuzziness, true, true, 0.0, false);
        KMeansDriver.run(directoryContainingConvertedInput,new Path(canopyOutput, "clusters-0-final"),output,0.01,10,true,0.01,false);
        ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new Path(output, "clusteredPoints"));
        System.out.println("points num:   " +  clusterDumper.getClusterIdToPoints().size()) ;
        System.out.println("maxPoints:  " + clusterDumper.getMaxPointsPerCluster()) ;
        System.out.println(clusterDumper.getTermDictionary()) ;
        System.out.println(clusterDumper.getNumTopFeatures()) ;
        //        clusterDumper.printClusters(null);

    }
   
    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf() ;
//        HdfsDAO hdfsDAO = new HdfsDAO(HDFS , conf) ;
//        hdfsDAO.ls(inPath);
//        hdfsDAO.rmr(inPath);
//        hdfsDAO.mkdirs(inPath);
//        hdfsDAO.copyFile(localFile , inPath);
        new FuzzyKmeansTest().runJob(new Path(inPath) ,  new Path(outPath) , 80,55,new EuclideanDistanceMeasure() ,
                0.5 , 10,2.0f);
    }

}
