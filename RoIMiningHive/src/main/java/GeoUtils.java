import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.distance.GeodesicSphereDistCalc;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.commons.math3.exception.DimensionMismatchException;
import java.util.List;

public class GeoUtils {

    final static double RAD = DistanceUtils.EARTH_MEAN_RADIUS_KM;
    final static SpatialContext ctx = SpatialContext.GEO;
    final static SpatialContext jtsctx = JtsSpatialContext.GEO;

    public static double distance(double[] a, double[] b) throws DimensionMismatchException {
        DistanceCalculator haversine = new GeodesicSphereDistCalc.Haversine();
        double dist_degree = haversine.distance((Point) getPoint(a[0], a[1]), (Point) getPoint(b[0], b[1]));
        double dist_rad = DistanceUtils.toRadians(dist_degree);
        return dist_rad * RAD * 1000;
    }

    public static Shape getPoint(double longitude, double latitude) {
        return ctx.makePoint(longitude, latitude);
    }

    public static double distance(Point a, Point b) throws DimensionMismatchException {
        DistanceCalculator haversine = new GeodesicSphereDistCalc.Haversine();
        double dist_degree = haversine.distance(a, b);
        double dist_rad = DistanceUtils.toRadians(dist_degree);
        return dist_rad * RAD * 1000;
    }

    public static Shape getPolygon(List<Point> points) throws InvalidShapeException {
        String tmp = "[[";
        for (Point point : points) {
            tmp += "[" + point.getX() + "," + point.getY() + "],";
        }
        tmp = tmp.substring(0, tmp.length() - 1);
        tmp += "]]";
        String shapeString = "{'type':'Polygon','coordinates':" + tmp + "}";
        return jtsctx.readShape(shapeString);
    }
}