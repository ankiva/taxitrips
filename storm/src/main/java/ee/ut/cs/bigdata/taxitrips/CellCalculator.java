package ee.ut.cs.bigdata.taxitrips;

import java.math.BigDecimal;

/**
 * Calculates cell numbers based on coordinates. 500m to south corresponds to change of 0.004491556.
 * 500m to the east corresponds to 0.005986 degrees.
 * Starting(north east) and ending(south west) points of the whole grid are the same for both queries.
 * Grid starting cell center point 41.474937, -74.913585.
 */
public class CellCalculator {

    private static final BigDecimal STARTING_LATITUDE = new BigDecimal("41.477182778");
    private static final BigDecimal STARTING_LONGITUDE = new BigDecimal("-74.916578");

    /**
     * Query 2 cell size is 250m x 250m and query 1 cell size is 500m x 500m.
     **/
    private static final int QUERY2_GRID_SIZE = 600;
    private static final int QUERY1_GRID_SIZE = 300;

    private static final BigDecimal M250_LATITUDE_CHANGE = new BigDecimal("0.002245778");
    private static final BigDecimal M250_LONGITUDE_CHANGE = new BigDecimal("0.002993");
    private static final BigDecimal M500_LATITUDE_CHANGE = new BigDecimal("0.004491556");
    private static final BigDecimal M500_LONGITUDE_CHANGE = new BigDecimal("0.005986");

    private static final BigDecimal ENDING_LATITUDE = STARTING_LATITUDE.subtract(M500_LATITUDE_CHANGE.multiply(BigDecimal.valueOf(QUERY1_GRID_SIZE)));
    private static final BigDecimal ENDING_LONGITUDE = STARTING_LONGITUDE.add(M500_LONGITUDE_CHANGE.multiply(BigDecimal.valueOf(QUERY1_GRID_SIZE)));

    public static Cell calculateQuery1Cell(BigDecimal latitude, BigDecimal longitude) {
        if (isPointWithinBoundaries(latitude, longitude)) {
            int rowNumber = calculateLatitudeChangeFromStart(latitude).divideToIntegralValue(M500_LATITUDE_CHANGE).intValue() + 1;
            int columnNumber = calculateLongitudeChangeFromStart(longitude).divideToIntegralValue(M500_LONGITUDE_CHANGE).intValue() + 1;
            return Cell.of(columnNumber, rowNumber);
        }
        return null;
    }

    public static Cell calculateQuery2Cell(BigDecimal latitude, BigDecimal longitude) {
        if (isPointWithinBoundaries(latitude, longitude)) {
            int rowNumber = calculateLatitudeChangeFromStart(latitude).divideToIntegralValue(M250_LATITUDE_CHANGE).intValue() + 1;
            int columnNumber = calculateLongitudeChangeFromStart(longitude).divideToIntegralValue(M250_LONGITUDE_CHANGE).intValue() + 1;
            return Cell.of(columnNumber, rowNumber);
        }
        return null;
    }

    private static BigDecimal calculateLatitudeChangeFromStart(BigDecimal latitude){
        return STARTING_LATITUDE.subtract(latitude);
    }

    private static BigDecimal calculateLongitudeChangeFromStart(BigDecimal longitude){
        return longitude.subtract(STARTING_LONGITUDE);
    }

    public static boolean isPointWithinBoundaries(BigDecimal latitude, BigDecimal longitude) {
        if(isLatitudeWithinBoundaries(latitude) && isLongitudeWithinBoundaries(longitude)){
            return true;
        }
//        System.out.println(latitude.toPlainString() + ", " + longitude.toPlainString() + " not within boundaries");
        return false;
    }

    private static boolean isLatitudeWithinBoundaries(BigDecimal latitude) {
        return latitude.compareTo(STARTING_LATITUDE) <= 0 && latitude.compareTo(ENDING_LATITUDE) > 0;
    }

    private static boolean isLongitudeWithinBoundaries(BigDecimal longitude) {
        return longitude.compareTo(STARTING_LONGITUDE) >= 0 && longitude.compareTo(ENDING_LONGITUDE) < 0;
    }
}
