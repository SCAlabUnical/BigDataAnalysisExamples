import java.util.List;

public class KDistanceCalculator {

    public static double calculateEps(List<Double> distances) {
        int A = 0, B = distances.size() - 1;
        int currentElbow = 0;
        int tmpElbow;
        while (true) {
            tmpElbow = getElbowPoint(distances, A, B);
            if (!(tmpElbow >= A && tmpElbow <= B))
                break;
            currentElbow = tmpElbow;
            int dim = B - A + 1;
            System.out.format("[%d,%d](S:%d)(E:%d)(d:%d)%n", A, B, dim, currentElbow, distances.get(currentElbow).intValue());
            if (dim <= 3)
                break;
            A = currentElbow - ((currentElbow - A) / 2);
            B = currentElbow + ((B - currentElbow) / 2);
        }
        return distances.get(currentElbow);
    }


    public static int getElbowPoint(List<Double> distances, int A, int B) {
        int dim = B - A + 1;

        // max variables
        int iMax = 0;
        double distMax = 0.0d;

        // temporary variables
        double xNorm;
        double yNorm;
        double tmpDist;

        double distB = distances.get(B);
        double distA = distances.get(A);
        double distI;
        int i = 0;
        for (Double distance : distances) {
            distI = distance;
            if (i >= A && i <= B) {
                xNorm = (i - A) * 1.0 / (dim - 1);
                yNorm = (1.0 * (distI - distB)) / (1.0 * (distA - distB));

                tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0;

                if (tmpDist > distMax) {
                    distMax = tmpDist;
                    iMax = i;
                }
            }
            i++;
        }
        return iMax;
    }
}
