package ec.ecust.edu;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
        try {
            Connection connection = DriverManager.getConnection(p.getProperty("url")
                    ,p.getProperty("username"), (String) p.get("password")) ;
            for(Cluster cluster:clusters){
                String sql = "instert into cluster values(?,?)" ;
                String id = String.valueOf(UUID.randomUUID());
                PreparedStatement ps = connection.prepareStatement(sql) ;
                ps.setString(1 , id);
                File file = new File("/tmp") ;
                OutputStream os = new FileOutputStream(file) ;
                DataOutputStream dis = new DataOutputStream(os) ;
                cluster.write(dis);
                dis.close();
                InputStream is = new FileInputStream(file) ;
                ps.setBinaryStream(2 , is , is.available());
            }
        } catch (SQLException e) {
            log.error("connection连接出错");
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        Path path = new Path("hdfs://localhost:8020/gp/result") ;
        List<Cluster> clusters = DisplayCluster.loadClustersWritable(path) ;
        new MysqlClusterSolver().solve(clusters);
    }
}














