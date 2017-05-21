package ec.ecust.edu.mysql;

import ec.ecust.edu.ClusterSolver;
import ec.ecust.edu.DisplayCluster;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * Created by pengfei on 2017/4/16.
 */
public class MysqlClusterSolver extends MysqlBase implements ClusterSolver {

    private final static Logger log = LoggerFactory.getLogger(MysqlClusterSolver.class) ;

    private static MysqlClusterSolver mysqlClusterSolver = new MysqlClusterSolver() ;
    private MysqlClusterSolver(){}
    public static  MysqlClusterSolver getInstance(){
        return mysqlClusterSolver ;
    }


    public void solve(List<Cluster> clusters) {
        try {
            Connection connection = getConnection() ;
            for(Cluster cluster:clusters){
                String sql = "instert into cluster values(?,?)" ;
                PreparedStatement ps = connection.prepareStatement(sql) ;
                String id = String.valueOf(cluster.getId());
                ps.setString(1 , id);
                writeDataToFile(cluster) ;
                InputStream is = new FileInputStream(file) ;
                ps.setBinaryStream(2 , is , is.available());
                ps.executeUpdate() ;
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

    public void saveIdex(String id , Integer clusterId){
        try {
            Connection connection = getConnection() ;
//            String sql_query = "select * from image_index where id = ?" ;
//            PreparedStatement ps_query = connection.prepareStatement(sql_query) ;
//            ResultSet rs = ps_query.executeQuery() ;
//            rs.next() ;
//            rs.getString("cluster_id") ;

            String sql = "instert into image_index values(?,?)" ;
            PreparedStatement ps = connection.prepareStatement(sql) ;
            ps.setString(1 , id);
            ps.setInt(2 , clusterId);
            ps.executeUpdate() ;
        } catch (SQLException e) {
            log.error("connection连接出错");
            e.printStackTrace();
        }
    }

    private void writeDataToFile(Cluster cluster)  {
        OutputStream os = null;
        try {
            os = new FileOutputStream(file);
            DataOutputStream dos = new DataOutputStream(os) ;
            cluster.write(dos);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                os.close();
            } catch (IOException e) {
                log.error("os 关闭异常");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Path path = new Path("hdfs://localhost:8020/gp/result") ;
        List<Cluster> clusters = DisplayCluster.loadClustersWritable(path) ;
        new MysqlClusterSolver().solve(clusters);
    }

    public void solve(Path seqIn, List<Cluster> clusters) {
        solve(clusters);
//        seqIn.
    }
}














