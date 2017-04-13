package ec.ecust.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List ;
/**
 * Created by pengfei on 2017/4/13.
 */
public class DisplayCluster {

    private static final Logger log = LoggerFactory.getLogger(DisplayCluster.class) ;


    
    public static List<Cluster> loadClustersWritable(Path output) throws IOException {
        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(output.toUri() , conf) ;
        List<Cluster> clusters = new ArrayList<Cluster>() ;
        for(FileStatus s:fs.listStatus(output , new ClusterFilter())){
            List<Cluster> cs = readClustersWritable(s.getPath()) ;
            clusters.addAll(cs) ;
        }
        return clusters ;
    }

    private static List<Cluster> readClustersWritable(Path path) {
        List<Cluster> clusters = new ArrayList<Cluster>() ;
        Configuration conf = new Configuration() ;
        for(ClusterWritable value:new SequenceFileDirValueIterable<ClusterWritable>(path , PathType.LIST,
                PathFilters.logsCRCFilter() , conf)){
            Cluster cluster = value.getValue() ;
            log.info("Reading Cluser:{}  center:{} numPoints {} radius{}",
                    cluster.getId() ,
                    AbstractCluster.formatVector(cluster.getCenter() , null),
                    cluster.getNumObservations() ,
                    AbstractCluster.formatVector(cluster.getRadius() , null)
            ) ;
            clusters.add(cluster) ;
        }
        return clusters ;
    }

}
