package ec.ecust.edu;

import ec.ecust.edu.mysql.MysqlClusterSolver;
import mahout.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List ;
import java.io.IOException;
import java.util.Map;

/**
 * Created by pengfei on 2017/4/13.
 */
public class KMeansManager {

    private KmConf kmConf ;
    private ClusterSolver solver ;
    private HdfsDAO hdfsDAO  ;
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";

    public KMeansManager(KmConf kmConf , ClusterSolver solver){
        this.kmConf = kmConf ;
        this.solver = solver ;
        Configuration conf = new Configuration() ;
        hdfsDAO = new HdfsDAO(kmConf.getHdfs(),conf) ;
    }

    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        Path seqIn = getSeqIn(kmConf.getInpath() , kmConf.isNeedSeq())  ;
        Path clusterIn = getCLusterIn(seqIn , kmConf.getClassification()) ;
        KMeansDriver.run(seqIn , clusterIn , new Path(kmConf.getOutpath()) , 0.01 ,
                kmConf.getMaxIterations() ,true ,
                kmConf.getClusterClassificationThreshold() ,
                false);

        List<Cluster> clusters = DisplayCluster.loadClustersWritable(new Path(kmConf.getOutpath()))  ;
        MysqlClusterSolver.getInstance().solve(clusters);
        saveFeatureVector(kmConf.getInpath()) ;
    }

    private void saveFeatureVector(String inPath ) throws IOException, ClassNotFoundException, InterruptedException {
        String seqFilePath = inPath+"/seqfile";
        Path seqFile = new Path(seqFilePath , DIRECTORY_CONTAINING_CONVERTED_INPUT+"2") ;  ;
        InputMapperV2.init();
        InputDriverV2.runJob(new Path(inPath),seqFile,"org.apache.mahout.math.RandomAccessSparseVector");
    }

    private Path getCLusterIn(Path seqIn, int classification) throws IOException {
        Path clustersSeeds = new Path(seqIn.getParent() , "/seeds");
        clustersSeeds = RandomSeedGenerator.buildRandom(new Configuration(),seqIn,
                clustersSeeds, classification, new EuclideanDistanceMeasure());
        return clustersSeeds ;
    }

    private Path getSeqIn(String inPath, boolean isNeedSeq) throws InterruptedException, IOException, ClassNotFoundException {
        String seqFilePath = inPath+"/seqfile";
        Path seqFile = new Path(seqFilePath , DIRECTORY_CONTAINING_CONVERTED_INPUT) ;
        if(isNeedSeq) {
            InputDriver.runJob(new Path(inPath), seqFile, "org.apache.mahout.math.RandomAccessSparseVector");
        }
        return seqFile ;
    }

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException, IOException{
        KMeansManager kmm = new KMeansManager(KmConf.getKmConf() , MysqlClusterSolver.getInstance()) ;
        kmm.run(); 
    }
}




















