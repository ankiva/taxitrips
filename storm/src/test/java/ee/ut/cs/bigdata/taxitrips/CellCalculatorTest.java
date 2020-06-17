package ee.ut.cs.bigdata.taxitrips;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

public class CellCalculatorTest {

//    CellCalculator calculator;

//    @BeforeClass
//    public void setup() {
//        calculator = new CellCalculator();
//    }

    @Test
    public void testQuery1StartingPoint() {
        BigDecimal latitude = new BigDecimal("41.477182778");
        BigDecimal longitude = new BigDecimal("-74.916578");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 1, "column number");
        Assert.assertEquals(result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell1_1Point() {
        BigDecimal latitude = new BigDecimal("41.477182777");
        BigDecimal longitude = new BigDecimal("-74.916577");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 1, "column number");
        Assert.assertEquals(result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell1_1LastPoint() {
        BigDecimal latitude = new BigDecimal("41.472691223");
        BigDecimal longitude = new BigDecimal("-74.910593");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 1, "column number");
        Assert.assertEquals(result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell2_2FirstPoint() {
        BigDecimal latitude = new BigDecimal("41.472691222");
        BigDecimal longitude = new BigDecimal("-74.910592");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 2, "column number");
        Assert.assertEquals(result.getRow(), 2, "row number");
    }

    @Test
    public void testQuery1EndingPointOutOfGrid() {
        BigDecimal latitude = new BigDecimal("40.129715978");
        BigDecimal longitude = new BigDecimal("-73.120778");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery1EndingPointInGrid() {
        BigDecimal latitude = new BigDecimal("40.129715979");
        BigDecimal longitude = new BigDecimal("-73.120779");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 300, "column number");
        Assert.assertEquals(result.getRow(), 300, "row number");
    }

    @Test
    public void testQuery1LatitudeOutOfGrid() {
        BigDecimal latitude = new BigDecimal("40");
        BigDecimal longitude = new BigDecimal("-74");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery1LongitudeOutOfGrid() {
        BigDecimal latitude = new BigDecimal("41");
        BigDecimal longitude = new BigDecimal("-75");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery2StartingPoint() {
        BigDecimal latitude = new BigDecimal("41.477182778");
        BigDecimal longitude = new BigDecimal("-74.916578");
        Cell result = CellCalculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 1, "column number");
        Assert.assertEquals(result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery2Cell600_600FirstPoint() {
        BigDecimal latitude = new BigDecimal("40.131961756");
        BigDecimal longitude = new BigDecimal("-73.123771");
        Cell result = CellCalculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 600, "column number");
        Assert.assertEquals(result.getRow(), 600, "row number");
    }

    @Test
    public void testQuery2Cell599_599LastPoint() {
        BigDecimal latitude = new BigDecimal("40.131961757");
        BigDecimal longitude = new BigDecimal("-73.123772");
        Cell result = CellCalculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals(result.getColumn(), 599, "column number");
        Assert.assertEquals(result.getRow(), 599, "row number");
    }

    @Test
    public void testQuery1RandomExample() {
        BigDecimal latitude = new BigDecimal("40.751266");
        BigDecimal longitude = new BigDecimal("-73.993973");
        Cell result = CellCalculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertTrue(result.getColumn() >= 1, "column number gte 1");
        Assert.assertTrue(result.getColumn() <= 300, "column number, lte 300");
        Assert.assertTrue(result.getRow() >= 1, "row number gte 1");
        Assert.assertTrue(result.getRow() <= 300, "row number lte 300");
    }

    @Test
    public void testQuery2RandomExample() {
        BigDecimal latitude = new BigDecimal("40.751266");
        BigDecimal longitude = new BigDecimal("-73.993973");
        Cell result = CellCalculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertTrue(result.getColumn() >= 1, "column number gte 1");
        Assert.assertTrue(result.getColumn() <= 600, "column number lte 600");
        Assert.assertTrue(result.getRow() >= 1, "row number gte 1");
        Assert.assertTrue(result.getRow() <= 600, "row number lte 600");
    }
}
