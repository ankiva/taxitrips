package ee.ut.cs.bigdata.taxitrips.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/* Spark streaming does not support windowing on event time */
public class ProfitableAreas {
    public void test() throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }

    public void execute() throws InterruptedException {

        //setMaster("local[2]")
        SparkConf conf = new SparkConf().setAppName("ProfitableAreas");

        SparkContext sc = SparkContext.getOrCreate(conf);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        JavaRDD<String> jrdd = jsc.textFile("file:///C:\\Users\\anti\\mag\\bigdata\\project\\testingdata\\testdata.csv");

        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(jrdd);
        System.out.println("queue created");

//        jsc.stop(false);
//        sc.stop();
//        System.out.println("stopped");

//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        StreamingContext stc = new StreamingContext(sc, Durations.seconds(1));
        JavaStreamingContext jssc = new JavaStreamingContext(stc);

        JavaDStream<String> stream = jssc.queueStream(queue);
//        JavaDStream<String> stream = jssc.textFileStream("file:///C:\\Users\\anti\\mag\\bigdata\\project\\testingdata");

        stream.print();

        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException {
        ProfitableAreas query = new ProfitableAreas();
        query.execute();
    }
}
