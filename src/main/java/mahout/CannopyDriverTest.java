package mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

/**
 * Created by pengfei on 2017/4/10.
 */
public class CannopyDriverTest {
    private static final String HDFS = "hdfs://localhost:8020";
    private static final String localFile = "src/main/resources/randomData.csv";
    private static String inPath = HDFS + "/kmeans/mix_data";
    private static String seqFilePath = inPath+"/seqfile";
    private static String outPath = inPath+"/result/";
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf() ;
//        HdfsDAO hdfsDAO = new HdfsDAO(HDFS , conf) ;
//        hdfsDAO.ls(inPath);
//        hdfsDAO.rmr(inPath);
//        hdfsDAO.mkdirs(inPath);
//        hdfsDAO.copyFile(localFile , inPath);
//        Path seqFile = new Path(seqFilePath , DIRECTORY_CONTAINING_CONVERTED_INPUT) ;
//        InputDriver.runJob(new Path(inPath) , seqFile , "org.apache.mahout.math.RandomAccessSparseVector");
////        SequenceFile.Writer writer = SequenceFile.createWriter(hdfsDAO.getFileSystem() ,
////                conf,seqFile , Text.class , Text.class) ;
//
//        CanopyDriver.run(conf , seqFile , new Path(outPath) , new EuclideanDistanceMeasure(),3 , 4 , true,0,false);
//        ClusterDumper clusterDumper = new ClusterDumper(new Path(outPath,
//                "clusters-0-final"), new Path(outPath, "clusteredPoints"));
//            clusterDumper.printClusters(null);
    	JobConf conf = new JobConf() ;
        HdfsDAO hdfsDAO = new HdfsDAO(HDFS , conf) ;
        hdfsDAO.ls(inPath);
        hdfsDAO.rmr(inPath);
        hdfsDAO.mkdirs(inPath);
        hdfsDAO.copyFile(localFile , inPath);
        Path seqFile = new Path(seqFilePath , DIRECTORY_CONTAINING_CONVERTED_INPUT) ;
        InputDriver.runJob(new Path(inPath) , seqFile , "org.apache.mahout.math.RandomAccessSparseVector");
//        SequenceFile.Writer writer = SequenceFile.createWriter(hdfsDAO.getFileSystem() ,
//                conf,seqFile , Text.class , Text.class) ;

        Configuration conf1 = new Configuration() ;
//        conf1.set("mapreduce.framework.name", "yarn");
//        conf1.set("yarn.resourcemanager.hostname", "hadoop");
//        conf1.set("fs.defaultFS", "hdfs://localhost:8020/");
        CanopyDriver.run(conf1 , seqFile , new Path(outPath) , new EuclideanDistanceMeasure(),3 , 4 , true,0,false);
    }
}










