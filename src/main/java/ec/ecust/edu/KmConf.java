package ec.ecust.edu;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by pengfei on 2017/4/13.
 */
@Data
public class KmConf {

    private static final Logger log = LoggerFactory.getLogger(KmConf.class) ;
    private static final String PROPERTI_NAME = "km.properties" ;
    private String hdfs = "hdfs://localhost:8020";
    private String inpath = hdfs + "/features";
    private String outpath = hdfs + "/result" ;
    private String clusterPath = "" ; 
    private int maxIterations  = 1 ;
    private int clusterClassificationThreshold = 10 ;
    private int classification = 10 ;
    private boolean isNeedSeq = true ;
    private String newFeaturePath = "/newFeatures" ;
    private boolean isNeedClusterIn=true ; 
    private boolean clustersSeeds = true ; 
    private boolean isNeedCluster = true ; 
    private static KmConf kmConf = new KmConf() ;
    public static KmConf getKmConf(){
        return kmConf ;
    }

    private KmConf(){
        Properties p = new Properties() ;
        InputStream in = KmConf.class.getClassLoader().getResourceAsStream(PROPERTI_NAME) ;
        try {
            p.load(in);
            if(p.containsKey(hdfs)){
                hdfs = (String) p.get(hdfs);
            }
            if(p.containsKey("isNeedClusterIn")){
            	isNeedClusterIn = Boolean.valueOf((String)p.get("isNeedClusterIn")) ; 
            }
            if(p.containsKey("isNeedCluster")){
            	isNeedCluster = Boolean.valueOf((String)p.get("isNeedCluster")) ; 
            }
            if(p.containsKey("inpath")){
                inpath = hdfs +  p.get("inpath") ;
            }
            if(p.containsKey("clustersSeeds")){
            	clustersSeeds = Boolean.valueOf((String)p.get("clustersSeeds")) ; 
            }
            if(p.containsKey("outpath")){
                outpath = hdfs +  p.get("outpath") ;
            }
            if(p.containsKey("clusterPath")){
            	clusterPath = hdfs + p.get("clusterPath" ) ; 
            }
            if(p.containsKey("maxIterations")) {
                maxIterations = Integer.parseInt((String) p.get("maxIterations"));
            }
            if(p.containsKey("clusterClassificationThreshold")){
                clusterClassificationThreshold = (Integer.valueOf((String) p.get("clusterClassificationThreshold")));
            }
            if(p.containsKey("classification")){
                classification = Integer.valueOf((String) p.get("classification"));
            }
            if(p.containsKey("isNeedSeq")){
                isNeedSeq = Boolean.valueOf((String)p.get("isNeedSeq")) ;
            }
            if(p.containsKey("newFeaturePath")){
                newFeaturePath = hdfs + (String)p.getProperty("newFeaturePath") ;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public boolean isClustersSeeds() {
		return clustersSeeds;
	}

	public void setClustersSeeds(boolean clustersSeeds) {
		this.clustersSeeds = clustersSeeds;
	}

	public boolean isNeedClusterIn() {
		return isNeedClusterIn;
	}

	public void setNeedClusterIn(boolean isNeedClusterIn) {
		this.isNeedClusterIn = isNeedClusterIn;
	}

	public static void main(String[] args){
        KmConf kmConf = new KmConf() ;
//        System.out.println(kmConf.getClassification()) ;
    }

    
	public String getNewFeaturePath() {
		return newFeaturePath;
	}

	public void setNewFeaturePath(String newFeaturePath) {
		this.newFeaturePath = newFeaturePath;
	}

	public boolean isNeedSeq() {
		return isNeedSeq;
	}
	

	public boolean isNeedCluster() {
		return isNeedCluster;
	}

	public void setNeedCluster(boolean isNeedCluster) {
		this.isNeedCluster = isNeedCluster;
	}

	public void setNeedSeq(boolean isNeedSeq) {
		this.isNeedSeq = isNeedSeq;
	}

	public String getHdfs() {
		return hdfs;
	}


	
	public String getClusterPath() {
		return clusterPath;
	}

	public void setClusterPath(String clusterPath) {
		this.clusterPath = clusterPath;
	}

	public void setHdfs(String hdfs) {
		this.hdfs = hdfs;
	}

	public String getInpath() {
		return inpath;
	}

	public void setInpath(String inpath) {
		this.inpath = inpath;
	}

	public String getOutpath() {
		return outpath;
	}

	public void setOutpath(String outpath) {
		this.outpath = outpath;
	}

	public int getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public int getClusterClassificationThreshold() {
		return clusterClassificationThreshold;
	}

	public void setClusterClassificationThreshold(int clusterClassificationThreshold) {
		this.clusterClassificationThreshold = clusterClassificationThreshold;
	}

	public int getClassification() {
		return classification;
	}

	public void setClassification(int classification) {
		this.classification = classification;
	}
    
}















