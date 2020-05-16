package ee.ut.cs.bigdata;

import java.math.BigDecimal;

/**
 * Calculates cell numbers based on coordinates. 500m to south corresponds to change of 0.004491556.
 * 500m to the east corresponds to 0.005986 degrees.
 * Starting(north east) and ending(south west) points of the whole grid are the same or both queries.
 */
public class CellCalculator {

    private static final BigDecimal STARTING_LATITUDE_CENTER = new BigDecimal("41.474937");
    private static final BigDecimal STARTING_LONGITUDE_CENTER = new BigDecimal("-74.913585");
    private static final BigDecimal STARTING_LATITUDE = new BigDecimal("41.472691222");
    private static final BigDecimal STARTING_LONGITUDE = new BigDecimal("-74.916578");

    /**
     * Query 2 cell size is 250m x 250m and query 1 cell size is 500m.
     **/
    private static final int QUERY2_GRID_SIZE = 600;
    private static final int QUERY1_GRID_SIZE = 300;

    private static final BigDecimal M250_LATITUDE_CHANGE = new BigDecimal("0.002245778");
    private static final BigDecimal M250_LONGITUDE_CHANGE = new BigDecimal("0.002993");
    private static final BigDecimal M500_LATITUDE_CHANGE = new BigDecimal("0.004491556");
    private static final BigDecimal M500_LONGITUDE_CHANGE = new BigDecimal("0.005986");


    private static final BigDecimal ENDING_LATITUDE = STARTING_LATITUDE.add(M500_LATITUDE_CHANGE.multiply(BigDecimal.valueOf(QUERY1_GRID_SIZE)));
    private static final BigDecimal ENDING_LONGITUDE = STARTING_LONGITUDE.add(M500_LONGITUDE_CHANGE.multiply(BigDecimal.valueOf(QUERY1_GRID_SIZE)));

    public Point calculateQuery1Cell(BigDecimal latitude, BigDecimal longitude) {
        if (isPointWithinBoundaries(latitude, longitude)) {
            int rowNumber = latitude.subtract(STARTING_LATITUDE).divideToIntegralValue(M500_LATITUDE_CHANGE).intValue() + 1;
            int columnNumber = longitude.subtract(STARTING_LONGITUDE).divideToIntegralValue(M500_LONGITUDE_CHANGE).intValue() + 1;
            return Point.of(columnNumber, rowNumber);
        }
        return null;
    }

    public Point calculateQuery2Cell(BigDecimal latitude, BigDecimal longitude) {
        if (isPointWithinBoundaries(latitude, longitude)) {
            int rowNumber = latitude.subtract(STARTING_LATITUDE).divideToIntegralValue(M250_LATITUDE_CHANGE).intValue() + 1;
            int columnNumber = longitude.subtract(STARTING_LONGITUDE).divideToIntegralValue(M250_LONGITUDE_CHANGE).intValue() + 1;
            return Point.of(columnNumber, rowNumber);
        }
        return null;
    }

    private boolean isPointWithinBoundaries(BigDecimal latitude, BigDecimal longitude) {
        return isLatitudeWithinBoundaries(latitude) && isLongitudeWithinBoundaries(longitude);
    }

    private boolean isLatitudeWithinBoundaries(BigDecimal latitude) {
        return latitude.compareTo(STARTING_LATITUDE) >= 0 && latitude.compareTo(ENDING_LATITUDE) < 0;
    }

    private boolean isLongitudeWithinBoundaries(BigDecimal longitude) {
        return longitude.compareTo(STARTING_LONGITUDE) >= 0 && longitude.compareTo(ENDING_LONGITUDE) < 0;
    }
}
