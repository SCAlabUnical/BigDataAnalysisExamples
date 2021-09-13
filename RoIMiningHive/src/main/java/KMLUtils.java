import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import java.util.*;

class KMLUtils {

    private static class ClockwiseCoordinateComparator implements Comparator<Coordinate> {
        private final Coordinate reference;

        public ClockwiseCoordinateComparator(Coordinate reference) {
            this.reference = reference;
        }

        @Override
        public int compare(Coordinate a, Coordinate b) {
            // Variables to Store the atans
            double aTanA, aTanB;

            // Fetch the atans
            aTanA = Math.atan2(a.y - reference.y, a.x - reference.x);
            aTanB = Math.atan2(b.y - reference.y, b.x - reference.x);

            // Determine next point in Clockwise rotation
            if (aTanA < aTanB)
                return -1;
            else if (aTanB < aTanA)
                return 1;
            return 0;
        }
    }

    public static String serialize(Shape shape) {
        return serialize(shape, false, null);
    }

    public static String serialize(Shape shape, boolean closeFile, Map<String, String> ext) {
        if (shape instanceof JtsGeometry) {
            Geometry geometry = ((JtsGeometry) shape).getGeom();
            if (geometry instanceof Point) {
                return serializePoint((Point) geometry, closeFile, ext);
            } else if (geometry instanceof Polygon) {
                return serializePolygon((Polygon) geometry, closeFile, ext);
            } else {
                throw new IllegalArgumentException("Geometry type [" + geometry.getGeometryType() + "] not supported");
            }
        } else if (shape instanceof Point) {
            return serializePoint((Point) shape, closeFile, ext);
        } else if (shape instanceof Rectangle) {
            return serializeRectangle((Rectangle) shape, closeFile, ext);
        } else {
            throw new IllegalArgumentException("Shape type [" + shape.getClass().getSimpleName() + "] not supported");
        }
    }

    private static String serializeRectangle(Rectangle rectangle, boolean closeFile, Map<String, String> extendedData) {
        StringBuilder sb = new StringBuilder();

        if (closeFile) {
            sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
        }

        double[][] points = new double[4][2];
        points[0][0] = rectangle.getMinX();
        points[0][1] = rectangle.getMinY();
        points[1][0] = rectangle.getMaxX();
        points[1][1] = rectangle.getMaxY();
        points[2][0] = rectangle.getMinX();
        points[2][1] = rectangle.getMaxY();
        points[3][0] = rectangle.getMaxX();
        points[3][1] = rectangle.getMinY();

        HashMap<String, String> ext = generateExtendedDataString(extendedData);
        sb.append("<Placemark>" + ext.get("data") + "<Polygon><outerBoundaryIs><LinearRing><coordinates>");
        sb.append(points[0][0] + "," + points[0][1] + " ");
        sb.append(points[3][0] + "," + points[3][1] + " ");
        sb.append(points[1][0] + "," + points[1][1] + " ");
        sb.append(points[2][0] + "," + points[2][1] + " ");
        sb.append("</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>");
        if (ext.containsKey("style"))
            sb.append(ext.get("style"));

        if (closeFile) {
            sb.append("</Document></kml>");
        }
        return sb.toString();
    }

    private static String serializePolygon(Polygon geometry, boolean closeFile, Map<String, String> extendedData) {
        StringBuilder sb = new StringBuilder();
        if (closeFile) {
            sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
        }

        HashMap<String, String> ext = generateExtendedDataString(extendedData);
        sb.append("<Placemark>" + ext.get("data")
                + "<Polygon><outerBoundaryIs><LinearRing><tessellate>0</tessellate><coordinates>");
        Coordinate[] coordinates = geometry.getCoordinates();
        List<Coordinate> pointsList = new LinkedList<>();

        double sumLat = 0;
        double sumLng = 0;
        for (Coordinate coordinate : coordinates) {
            pointsList.add(coordinate);
            sumLat += coordinate.y;
            sumLng += coordinate.x;
        }
        Coordinate reference = new Coordinate(sumLng / coordinates.length, sumLat / coordinates.length);
        pointsList.sort(new ClockwiseCoordinateComparator(reference));
        Coordinate tmp = pointsList.remove(coordinates.length - 1);
        pointsList.add(0, tmp);
        for (Coordinate c : pointsList) {
            sb.append(c.x + "," + c.y + ",0.0 ");
        }
        sb.append("</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>");
        sb.append(ext.get("style"));
        if (closeFile) {
            sb.append("</Document></kml>");
        }
        return sb.toString();
    }

    private static String serializePoint(Point geometry, boolean closeFile, Map<String, String> extendedData) {
        StringBuilder sb = new StringBuilder();
        if (closeFile) {
            sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
        }
        HashMap<String, String> ext = generateExtendedDataString(extendedData);
        sb.append("<Placemark>" + ext.get("data") + "<Point><coordinates>" + geometry.getX() + "," + geometry.getY()
                + "</coordinates></Point></Placemark>");
        if (closeFile) {
            sb.append("</Document></kml>");
        }
        return sb.toString();
    }

    private static HashMap<String, String> generateExtendedDataString(Map<String, String> extendedData) {
        String ext = "";
        String preText = "";
        HashMap<String, String> ret = new HashMap<>();
        if (extendedData != null && extendedData.size() > 0) {
            ext = "<ExtendedData>";
            for (Map.Entry<String, String> entry : extendedData.entrySet()) {
                switch (entry.getKey()) {
                    case "styleUrl":
                        preText += "<styleUrl>" + entry.getValue().trim() + "</styleUrl>";
                        break;
                    case "color":
                        preText += "<styleUrl>#poly-" + entry.getValue().trim() + "</styleUrl>";
                        String style = "<Style id=\"poly-" + entry.getValue().trim() + "\">" + "<LineStyle>" + "<color>"
                                + entry.getValue().trim() + "</color>" + "	<width>2</width>" + "</LineStyle>"
                                + "<PolyStyle>" + "<color>" + entry.getValue().trim() + "</color>" + "	<fill>1</fill>"
                                + "<outline>1</outline>" + "</PolyStyle></Style>";
                        ret.put("style", style);
                        break;
                    case "description":
                        preText += "<description><![CDATA[descrizione:" + entry.getValue() + "]]></description>";
                        break;
                    case "name":
                        preText += "<name>" + entry.getValue() + "</name>";
                        break;
                    default:
                        ext += "<Data name=\"" + entry.getKey() + "\"><value>" + entry.getValue() + "</value></Data>";
                        break;
                }
            }
            ext += "</ExtendedData>";
        }
        ret.put("data", preText + ext);
        return ret;
    }
}