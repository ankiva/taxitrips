package ee.ut.cs.bigdata.spark.structured;

import ee.ut.cs.bigdata.CellCalculator;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProfitableAreas {

    public static final DataType SCHEMA_MONEY_TYPE = DataTypes.createDecimalType(7, 2);
    public static final DataType SCHEMA_COORDINATE_TYPE = DataTypes.createDecimalType(8, 6);

    private static void wordCount() throws StreamingQueryException {
        System.out.println("starting app");
        SparkSession spark = SparkSession
                .builder()
                .appName("ProfitableAreas")
                .getOrCreate();

        System.out.println("session created");
        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        System.out.println("lines loaded");
        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        System.out.println("mapped");
        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();
        System.out.println("aggregated");

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        System.out.println("waiting termination");
        query.awaitTermination();
        System.out.println("terminated");
    }

    private static StructType readInSchema(SparkSession spark) {
        return spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("C:\\Users\\anti\\mag\\bigdata\\project\\testingdata")
                .schema();
    }

    private static Dataset<Row> readLinesAsStream(SparkSession spark, StructType schema) {
        return spark
                .readStream()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .load("C:\\Users\\anti\\mag\\bigdata\\project\\testingdata");
    }

    /*
    Read in without schema. option spark.sql.streaming.schemaInference must be specified for spark session
    .config("spark.sql.streaming.schemaInference", "true")
     */

    private static Dataset<Row> readLinesAsStream(SparkSession spark) {
        return spark
                .readStream()
                .option("header", "true")
                .csv("C:\\Users\\anti\\mag\\bigdata\\project\\testingdata");
    }
    private static StructType defineSchema() {
        return new StructType()
                .add("medallion", DataTypes.StringType, true)
                .add("hack_license", DataTypes.StringType, true)
                .add("pickup_datetime", DataTypes.TimestampType, true)
                .add("dropoff_datetime", DataTypes.TimestampType, true)
                .add("trip_time_in_secs", DataTypes.IntegerType, true)
                .add("trip_distance", SCHEMA_COORDINATE_TYPE, true)
                .add("pickup_longitude", SCHEMA_COORDINATE_TYPE, true)
                .add("pickup_latitude", SCHEMA_COORDINATE_TYPE, true)
                .add("dropoff_longitude", SCHEMA_COORDINATE_TYPE, true)
                .add("dropoff_latitude", SCHEMA_COORDINATE_TYPE, true)
                .add("payment_type", DataTypes.StringType, true)
                .add("fare_amount", SCHEMA_MONEY_TYPE, true)
                .add("surcharge", SCHEMA_MONEY_TYPE, true)
                .add("mta_tax", SCHEMA_MONEY_TYPE, true)
                .add("tip_amount", SCHEMA_MONEY_TYPE, true)
                .add("tolls_amount", SCHEMA_MONEY_TYPE, true)
                .add("total_amount", SCHEMA_MONEY_TYPE, true);
    }

    private void query() throws StreamingQueryException {
        System.out.println("starting app");
        Instant start = Instant.now();

        SparkSession spark = SparkSession
                .builder()
                .appName("ProfitableAreas")
                .getOrCreate();

        System.out.println("session created");
//        StructType schema = readInSchema(spark);
        StructType schema = defineSchema();
        System.out.println("schema " + schema);

        Dataset<Row> lines = readLinesAsStream(spark, schema);

        System.out.println("lines loaded");

        lines = filter(lines);

        lines = calculateGridCells(lines);

        Dataset<Row> profitLines = profitQuery(lines);

        RelationalGroupedDataset emptyTaxisWindowGroup = lines.groupBy(
                functions.window(lines.col("dropoff_datetime"), "30 seconds", "5 seconds"),
                lines.col("ending_cell"));

        //TODO select only maxdropofftime of medallion
        Dataset<Row> emptyTaxisLines = emptyTaxisWindowGroup.agg(functions.count("medallion"), functions.approx_count_distinct("medallion"));
        emptyTaxisLines = emptyTaxisLines.withColumnRenamed("approx_count_distinct(medallion)", "nroftaxis");

//        emptyTaxisLines = emptyTaxisLines.withColumn("windowendtime", emptyTaxisLines.col("window.end"));

        //multiple aggregations not supported yet by spark structured streaming
//        Dataset<Row> joined = profitLines.as("profitstream").join(emptyTaxisLines.as("emptytaxisstream"),
//                functions.expr("profitstream.starting_cell = emptytaxisstream.ending_cell AND profitstream.window.end = emptytaxisstream.window.end"));

        StreamingQuery query = profitLines.writeStream()
                .outputMode("complete")
//                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .start();

        System.out.println("waiting termination");
        query.awaitTermination();
        System.out.println("terminated");
    }

    private static Dataset<Row> filter(Dataset<Row> lines) {
        lines = lines.filter((Row row) -> row.getDecimal(7) != null && row.getDecimal(6) != null
                && CellCalculator.isPointWithinBoundaries(row.getDecimal(7), row.getDecimal(6)));

        lines = lines.filter((Row row) -> row.getDecimal(9) != null && row.getDecimal(8) != null
                && CellCalculator.isPointWithinBoundaries(row.getDecimal(9), row.getDecimal(8)));

        lines = lines.filter((Row row) -> row.getDecimal(11) != null && row.getDecimal(11).signum() >= 0
                && row.getDecimal(14) != null && row.getDecimal(14).signum() >= 0);

        return lines;
    }

    private static Dataset<Row> calculateGridCells(Dataset<Row> lines) {
        Dataset<Row> cellCalulcatedLines = lines;
        cellCalulcatedLines = cellCalulcatedLines.withColumn("starting_cell", functions.udf(
                (BigDecimal latitude, BigDecimal longitude) -> CellCalculator.calculateQuery2Cell(latitude, longitude).toString(), DataTypes.StringType)
                .apply(cellCalulcatedLines.col("pickup_latitude"), cellCalulcatedLines.col("pickup_longitude")));

        cellCalulcatedLines = cellCalulcatedLines.withColumn("ending_cell", functions.udf(
                (BigDecimal latitude, BigDecimal longitude) -> CellCalculator.calculateQuery2Cell(latitude, longitude).toString(), DataTypes.StringType)
                .apply(cellCalulcatedLines.col("dropoff_latitude"), cellCalulcatedLines.col("dropoff_longitude")));

        return cellCalulcatedLines;
    }

    private static Dataset<Row> profitQuery(Dataset<Row> lines) {
        Dataset<Row> profitLines = lines.withColumn("profit", functions.expr("fare_amount + tip_amount"));

        RelationalGroupedDataset profitWindowGroup = profitLines.groupBy(
                functions.window(profitLines.col("dropoff_datetime"), "15 seconds", "5 seconds"),
                profitLines.col("starting_cell"));

        Dataset<Row> profitWindowAgg = profitWindowGroup.agg(functions.count("*"), functions.collect_list("profit"))
                .withColumnRenamed("collect_list(profit)", "profitlist")
                .withColumnRenamed("count(1)", "nr_of_trips");

        profitWindowAgg = profitWindowAgg.withColumn("median_profit", functions.udf((WrappedArray<BigDecimal> profits) -> calculateMedian(profits.iterator()), ProfitableAreas.SCHEMA_MONEY_TYPE)
                .apply(profitWindowAgg.col("profitlist")));

        return profitWindowAgg;
    }

    private static BigDecimal calculateMedian(Iterator<BigDecimal> iterator) {
        List<BigDecimal> profits = new ArrayList<>();
        while (iterator.hasNext()) {
            profits.add(iterator.next());
        }
        if (profits.size() > 0) {
            if (profits.size() > 1) {
                profits.sort(ComparatorUtils.naturalComparator());
                return profits.get(profits.size() / 2);
            } else {
                return profits.get(0);
            }
        }
        return null;
    }

    public static void main(String[] args) throws StreamingQueryException {
        ProfitableAreas query = new ProfitableAreas();
        query.query();
    }
}
