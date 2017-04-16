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
    private int maxIterations  = 10 ;
    private int clusterClassificationThreshold = 10 ;
    private int classification = 10 ;
    public KmConf(){
        Properties p = new Properties() ;
        InputStream in = KmConf.class.getClassLoader().getResourceAsStream(PROPERTI_NAME) ;
        try {
            p.load(in);
            if(p.containsKey(hdfs)){
                hdfs = (String) p.get(hdfs);
            }
            if(p.containsKey("inpath")){
                inpath = hdfs +  p.get("inpath") ;
            }
            if(p.containsKey("outpath")){
                outpath = hdfs +  p.get("outpath") ;
            }
            if(p.containsKey("maxIterations")) {
                maxIterations = (Integer) p.get("maxIterations");
            }
            if(p.containsKey("clusterClassificationThreshold")){
                clusterClassificationThreshold = (Integer.valueOf((String) p.get("clusterClassificationThreshold")));
            }
            if(p.containsKey("classification")){
                classification = Integer.valueOf((String) p.get("classification"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        KmConf kmConf = new KmConf() ;
//        System.out.println(kmConf.getClassification()) ;
    }

	public String getHdfs() {
		return hdfs;
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















