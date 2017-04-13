package mahout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.canopy.Canopy;
import org.apache.mahout.clustering.canopy.CanopyClusterer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class CanopyTest {
    //创建一个二维点集的向量组
    public static final double[][] points = { { 1, 1 }, { 2, 1 }, { 1, 2 }, { 2, 2 }, { 3, 3 }, { 8, 8 }, { 9, 8 },
            { 8, 9 }, { 9, 9 }, { 5, 5 }, { 5, 6 }, { 6, 6 } };
    
    // 创建简单的向量
    public static List<Vector> getPointVectors(double[][] raw) {
        List<Vector> points = new ArrayList<Vector>();
        double[] value = null;
        Vector vec = null;
        for (int i = 0; i < raw.length; i++) {
            value = raw[i];
            vec = new RandomAccessSparseVector(value.length);
            vec.assign(value);// 将数据存放在创建的 Vector中
            points.add(vec);
        }
        return points;
    }
    
    //Canopy 聚类算法的内存实现
    public static void canopyClusterInMemory() {
        // 设置距离阈值 T1,T2
        double T1 = 4.0;
        double T2 = 3.0;
        // 调用 CanopyClusterer.createCanopies 方法创建 Canopy，参数分别是：
        // 1. 需要聚类的点集
        // 2. 距离计算方法
        // 3. 距离阈值 T1 和 T2
        List<Canopy> canopies = CanopyClusterer.createCanopies(CanopyTest.getPointVectors(CanopyTest.points),
                new EuclideanDistanceMeasure(), T1, T2);
        // 打印创建的 Canopy，因为聚类问题很简单，所以这里没有进行下一步精确的聚类。
        // 有必须的时候，可以拿到 Canopy 聚类的结果作为 K 均值聚类的输入，能更精确更高效的解决聚类问题
        for (Canopy canopy : canopies) {
            System.out.println("Cluster id: " + canopy.getId() + " center: " + canopy.getCenter().asFormatString());
            System.out.println("Points: " + canopy.getNumObservations());
        }
    }
    

    
    public static void main(String[] args) throws IOException {
        CanopyTest.canopyClusterInMemory();
    }

}
