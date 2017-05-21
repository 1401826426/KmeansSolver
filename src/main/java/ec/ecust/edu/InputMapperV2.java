package ec.ecust.edu;

import com.google.common.collect.Lists;
import ec.ecust.edu.mysql.MysqlClusterSolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import java.util.List ;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by pengfei on 2017/5/20.
 */
public class InputMapperV2 extends Mapper<LongWritable, Text, Text, VectorWritable> {
    private static final Pattern SPACE = Pattern.compile(" ");
    private Constructor<?> constructor;
    private static List<Cluster> clusters = new ArrayList<Cluster>() ;
    private static MysqlClusterSolver mysqlClusterSolver ;
    private static KmConf kmConf = KmConf.getKmConf() ;

    public static void init() throws IOException {
        clusters = DisplayCluster.loadClustersWritable(new Path(kmConf.getOutpath())) ;
        mysqlClusterSolver = MysqlClusterSolver.getInstance() ;
    }
    public InputMapperV2() {
    }

    protected void map(LongWritable key, Text values, Mapper<LongWritable, Text, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
        String[] numbers = SPACE.split(values.toString());
        String resultKey = numbers[0] ;
        Collection<Double> doubles = Lists.newArrayList();
        String[] arr$ = numbers;
        int index = numbers.length;
        for(int i$ = 1; i$ < index; ++i$) {
            String value = arr$[i$];
            if(value.length() != 0)
                doubles.add(Double.valueOf(value));
        }

        if(!doubles.isEmpty()) {
            try {
                Vector result = (Vector)this.constructor.newInstance(new Object[]{Integer.valueOf(doubles.size())});
                index = 0;
                Iterator i$ = doubles.iterator();

                while(i$.hasNext()) {
                    Double d = (Double)i$.next();
                    result.set(index++, d.doubleValue());
                }

                VectorWritable vectorWritable = new VectorWritable(result);
                int clusterId = getClusterId(vectorWritable) ;
                mysqlClusterSolver.saveIdex(resultKey , clusterId);
                context.write(new Text(String.valueOf(index)), vectorWritable);
            } catch (InstantiationException var10) {
                throw new IllegalStateException(var10);
            } catch (IllegalAccessException var11) {
                throw new IllegalStateException(var11);
            } catch (InvocationTargetException var12) {
                throw new IllegalStateException(var12);
            }
        }

    }

    private int getClusterId(VectorWritable vector) {
        double result = 0 ;
        int resultId = 0  ;
        for(Cluster cluster :clusters){
            double tmp = cluster.pdf(vector) ;
            if(tmp > result){
                result = tmp ;
                resultId = cluster.getId() ;
            }
        }
        return resultId ;

    }

    protected void setup(Mapper<LongWritable, Text, Text, VectorWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        String vectorImplClassName = conf.get("vector.implementation.class.name");

        try {
            Class<? extends Vector> outputClass = conf.getClassByName(vectorImplClassName).asSubclass(Vector.class);
            this.constructor = outputClass.getConstructor(new Class[]{Integer.TYPE});
        } catch (NoSuchMethodException var5) {
            throw new IllegalStateException(var5);
        } catch (ClassNotFoundException var6) {
            throw new IllegalStateException(var6);
        }
    }
}
