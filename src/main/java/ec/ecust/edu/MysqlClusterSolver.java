package ec.ecust.edu;

import org.apache.mahout.clustering.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * Created by pengfei on 2017/4/16.
 */
public class MysqlClusterSolver implements ClusterSolver{
    private final static Properties p = new Properties() ;
    private final static Logger log = LoggerFactory.getLogger(MysqlClusterSolver.class) ;

    static{
        InputStream in = MysqlClusterSolver.class.getClassLoader()
                .getResourceAsStream("jdbc.properties") ;
        try {
            p.load(in) ;
            log.info("===========加载jdbc properties驱动:==============") ;
            Enumeration enumeration = p.propertyNames() ;
            while(enumeration.hasMoreElements()){
                String key = (String) enumeration.nextElement();
                log.info(key + "=" + p.get(key)) ;
            }
            log.info("================================================") ;
            Class.forName(p.getProperty("driverClass")) ;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void solve(List<Cluster> clusters) {
         for(Cluster cluster:clusters){

         }
    }

    public static void main(String[] args){

    }
}














