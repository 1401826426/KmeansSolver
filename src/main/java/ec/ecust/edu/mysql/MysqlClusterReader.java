package ec.ecust.edu.mysql;

import org.apache.log4j.Logger;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.DistanceMeasureCluster;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List ;
/**
 * Created by pengfei on 2017/4/21.
 */
public class MysqlClusterReader extends MysqlBase {

    private final static Logger log = Logger.getLogger(MysqlClusterReader.class) ;

    public List<Cluster> readAllFromMysql(){
        List<Cluster> clusters = new ArrayList<Cluster>() ;
        try {
            Connection connection = getConnection() ;
            String sql = "select `id` , `cluster` from cluster" ;
            Statement st = connection.createStatement() ;
            ResultSet rs = st.executeQuery(sql) ;
            while(rs.next()){
                Blob b = rs.getBlob("cluster") ;
                String id = rs.getString("id") ;
                log.info("get cluster " + id) ;
                writeDataToFile(b) ;
                DataInputStream dis = new DataInputStream(new FileInputStream(file)) ;
                Cluster cluster = new DistanceMeasureCluster() ;
                cluster.readFields(dis);
                clusters.add(cluster) ;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        return clusters ;

    }

    private void writeDataToFile(Blob b) {
        InputStream is = null ;
        OutputStream os = null ;
        try {
            is = b.getBinaryStream() ;
            os = new FileOutputStream(file) ;
            byte[] bytes = new byte[1024] ;
            int len ;
            while((len=is.read(bytes)) != -1){
                os.write(bytes , 0,len);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                is.close();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        List<Cluster> clusters = new MysqlClusterReader().readAllFromMysql() ;
        for(Cluster cluster:clusters){
            log.info(cluster.asFormatString(null)) ;
        }
    }

}
