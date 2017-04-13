package ec.ecust.edu;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by pengfei on 2017/4/13.
 */
public class ClusterFilter implements PathFilter {
    public boolean accept(Path path) {
        return path.getName().startsWith("clusters-") ;
    }
}
