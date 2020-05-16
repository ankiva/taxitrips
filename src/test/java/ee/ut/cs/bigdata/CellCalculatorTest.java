package ee.ut.cs.bigdata;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;

public class CellCalculatorTest {

    CellCalculator calculator;

    @BeforeClass
    public void setup() {
        calculator = new CellCalculator();
    }

    @Test
    public void testQuery1StartingPoint() {
        BigDecimal latitude = new BigDecimal("41.472691222");
        BigDecimal longitude = new BigDecimal("-74.916578");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 1, "column number");
        Assert.assertEquals((int) result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell1_1Point() {
        BigDecimal latitude = new BigDecimal("41.472691223");
        BigDecimal longitude = new BigDecimal("-74.916577");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 1, "column number");
        Assert.assertEquals((int) result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell1_1LastPoint() {
        BigDecimal latitude = new BigDecimal("41.477182777");
        BigDecimal longitude = new BigDecimal("-74.910593");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 1, "column number");
        Assert.assertEquals((int) result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery1Cell2_2FirstPoint() {
        BigDecimal latitude = new BigDecimal("41.477182778");
        BigDecimal longitude = new BigDecimal("-74.910592");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 2, "column number");
        Assert.assertEquals((int) result.getRow(), 2, "row number");
    }

    @Test
    public void testQuery1EndingPointOutOfGrid() {
        BigDecimal latitude = new BigDecimal("42.820158022");
        BigDecimal longitude = new BigDecimal("-73.120778");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery1EndingPointInGrid() {
        BigDecimal latitude = new BigDecimal("42.820158021");
        BigDecimal longitude = new BigDecimal("-73.120779");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 300, "column number");
        Assert.assertEquals((int) result.getRow(), 300, "row number");
    }

    @Test
    public void testQuery1LatitudeOutOfGrid() {
        BigDecimal latitude = new BigDecimal("43");
        BigDecimal longitude = new BigDecimal("-74");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery1LongitudeOutOfGrid() {
        BigDecimal latitude = new BigDecimal("42");
        BigDecimal longitude = new BigDecimal("-75");
        Point result = calculator.calculateQuery1Cell(latitude, longitude);
        Assert.assertNull(result, "point");
    }

    @Test
    public void testQuery2StartingPoint() {
        BigDecimal latitude = new BigDecimal("41.472691222");
        BigDecimal longitude = new BigDecimal("-74.916578");
        Point result = calculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 1, "column number");
        Assert.assertEquals((int) result.getRow(), 1, "row number");
    }

    @Test
    public void testQuery2Cell600_600FirstPoint() {
        BigDecimal latitude = new BigDecimal("42.817912244");
        BigDecimal longitude = new BigDecimal("-73.123771");
        Point result = calculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 600, "column number");
        Assert.assertEquals((int) result.getRow(), 600, "row number");
    }

    @Test
    public void testQuery2Cell599_599LastPoint() {
        BigDecimal latitude = new BigDecimal("42.817912243");
        BigDecimal longitude = new BigDecimal("-73.123772");
        Point result = calculator.calculateQuery2Cell(latitude, longitude);
        Assert.assertNotNull(result, "point");
        Assert.assertEquals((int) result.getColumn(), 599, "column number");
        Assert.assertEquals((int) result.getRow(), 599, "row number");
    }
}
