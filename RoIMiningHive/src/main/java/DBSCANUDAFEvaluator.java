import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

// Define an UDAF to run the DBSCAN clustering algorithm
public class DBSCANUDAFEvaluator implements UDAFEvaluator {

    private static class ClusterPoint extends PointImpl implements Clusterable {
        public ClusterPoint(double x, double y, SpatialContext ctx) {
            super(x, y, ctx);
        }

        @Override
        public double[] getPoint() {
            return new double[]{this.getX(), this.getY()};
        }
    }

    LinkedList<ClusterPoint> RoIPts = null;

    @Override
    public void init() {
        // The points of a RoI form a cluster
        RoIPts = new LinkedList<ClusterPoint>();
    }

    public boolean iterate(double latitude, double longitude) throws HiveException {
        // Build a point of a cluster using latitude and longitude
        ClusterPoint p = new ClusterPoint(longitude, latitude, SpatialContext.GEO);
        RoIPts.add(p);
        return true;
    }

    public LinkedList<ClusterPoint> terminatePartial() {
        return RoIPts;
    }

    public boolean merge(LinkedList<ClusterPoint> otherRoIPts) {
        // Merge intermediate results
        if (otherRoIPts == null) return true;
        RoIPts.addAll(otherRoIPts);
        return true;
    }

    public Text terminate() throws IOException, ParseException {
        int minPts = 2;
        List<Double> dists = calculateKNN(RoIPts, minPts);
        double eps = KDistanceCalculator.calculateEps(dists);
        DBSCANRoI<ClusterPoint> dbscan = new DBSCANRoI<>(eps, 2);
        List<Cluster<ClusterPoint>> clusters = dbscan.cluster(RoIPts);
        // Return the cluster with the highest support as a KML string
        String s = getMaxCluster(clusters, minPts);
        return new Text(s);
    }

    private String getMaxCluster(List<Cluster<ClusterPoint>> clusters, int minPts) throws IOException, ParseException {
        int max = 0;
        Cluster<ClusterPoint> cMax = null;
        for (Cluster<ClusterPoint> cluster : clusters) {
            if (cluster.getPoints().size() > minPts && cluster.getPoints().size() > max) {
                max = cluster.getPoints().size();
                cMax = cluster;
            }
        }
        if (cMax != null) {
            List<Point> pp = new LinkedList<>(cMax.getPoints());
            Shape shapeMax = GeoUtils.getPolygon(pp);
            return KMLUtils.serialize(shapeMax);
        }
        return null;
    }

    private static List<Double> calculateKNN(Collection<ClusterPoint> points, int numNeighbours) {
        List<Double> ret = new LinkedList<>();
        List<Double> tmp = new LinkedList<>();
        double distance;
        for (Point p1 : points) {
            tmp.clear();
            for (Point p2 : points) {
                if (!p1.equals(p2)) {
                    distance = GeoUtils.distance(p1, p2);
                    tmp.add(distance);
                }
            }
            Collections.sort(tmp);
            ret.add(tmp.get(numNeighbours));
        }
        ret.sort(Collections.reverseOrder());
        return ret;
    }
}