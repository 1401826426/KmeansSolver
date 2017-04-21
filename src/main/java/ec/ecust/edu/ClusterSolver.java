package ec.ecust.edu;

import org.apache.mahout.clustering.Cluster;

import java.util.List ;
/**
 * Created by pengfei on 2017/4/16.
 */
public interface ClusterSolver {
     void solve(List<Cluster> clusters);
}
