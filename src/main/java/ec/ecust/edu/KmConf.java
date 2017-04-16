package ec.ecust.edu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by pengfei on 2017/4/13.
 */
public class KmConf {

    private static final Logger log = LoggerFactory.getLogger(KmConf.class) ;
    private static final String DEFAULT_HDFS = "hdfs://localhost:8020";

    private static final String DEAFULT_INPATH = DEFAULT_HDFS + "/features";

    private static final String DEFAULT_OUTPATH = DEFAULT_HDFS + "/result" ;
    private static final Integer DEFAULT_MAX_ITERATIONS = 10 ;
    private static final Integer DEFAULT_CLUSTER_CLASSIFICATION_THRESHOLD = 10 ;
    private static final String PROPERTI_NAME = "km.properties" ;
    private String hdfs = DEFAULT_HDFS ;
    private String inpath = DEAFULT_INPATH ;
    private String outpath = DEFAULT_OUTPATH ;
    private int maxIterations  = DEFAULT_MAX_ITERATIONS;
    private int clusterClassificationThreshold = DEFAULT_CLUSTER_CLASSIFICATION_THRESHOLD;
    public KmConf(){
        Properties p = new Properties() ;
        InputStream in = KmConf.class.getClassLoader().getResourceAsStream(PROPERTI_NAME) ;
        try {
            p.load(in);
            if(p.containsKey(hdfs)){
                hdfs = (String) p.get(hdfs);
            }
            if(p.containsKey(inpath)){
                inpath = hdfs +  p.get(inpath) ;
            }
            if(p.containsKey(outpath)){
                outpath = hdfs +  p.get(outpath) ;
            }
            if(p.containsKey(maxIterations)) {
                maxIterations = (Integer) p.get(maxIterations);
            }
            if(p.containsKey(clusterClassificationThreshold)){
                clusterClassificationThreshold = (Integer) p.get(clusterClassificationThreshold) ;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}















