package mahout;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class KmeansHadoop {
    
    private static final String HDFS = "hdfs://localhost:8020";
    private static final String localFile = "src/main/resources/randomData.csv";
    private static String inPath = HDFS + "/kmeans/mix_data";
    private static String seqFilePath = inPath+"/seqfile";
    private static String seeds = inPath+"/seeds";
    private static String outPath = inPath+"/result/";
    private static String clusteredPoints = outPath +"/clusteredPoints";
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdirs(inPath);
        hdfs.copyFile(localFile, inPath);
//        hdfs.ls(inPath);
        Path seqFile = new Path(seqFilePath , DIRECTORY_CONTAINING_CONVERTED_INPUT) ; 
        InputDriver.runJob(new Path(inPath),seqFile,"org.apache.mahout.math.RandomAccessSparseVector");
        int k = 3;
        Path clustersSeeds = new Path(seeds);
        DistanceMeasure measure = new EuclideanDistanceMeasure();
        clustersSeeds = RandomSeedGenerator.buildRandom(conf,seqFile,clustersSeeds, k, measure);
//        KMeansDriver.run(new Path(inPath),seqFile,clustersSeeds,0.01,10,true,0.01,false);
        KMeansDriver.run(seqFile , clustersSeeds , new Path(outPath) , 0.01 , 10 , true , 0.01 , false);
//        FuzzyKMeansDriver
        Path outGlobPath = new Path(outPath, "clusters-*-final");
        Path clusteredPointsPath = new Path(clusteredPoints);
        System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath, clusteredPointsPath);

        ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
        //clusterDumper.printClusters(null);
        displayCluster(clusterDumper);
        
    }
    public static void displayCluster(ClusterDumper clusterDumper) {
        Iterator<Integer> keys = clusterDumper.getClusterIdToPoints().keySet().iterator();
        while (keys.hasNext()) {
            Integer center = keys.next();
            System.out.println("Center:" + center);
            for (WeightedVectorWritable point : clusterDumper.getClusterIdToPoints().get(center)) {
                Vector v = point.getVector();
                System.out.println(v.get(0) + "" + v.get(1));
            }
        }
    }
    

}
