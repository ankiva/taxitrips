package ee.ut.cs.bigdata.taxitrips.storm.stripes;

import ee.ut.cs.bigdata.taxitrips.storm.*;
import ee.ut.cs.bigdata.taxitrips.storm.bolt.DataFilterBolt;
import ee.ut.cs.bigdata.taxitrips.storm.bolt.CsvFileOutputBolt;
import ee.ut.cs.bigdata.taxitrips.storm.spout.CsvFileSourceSpout;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class ProfitableAreasTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new ProfitableAreasTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        String dataFileName;
        String outputFileName = "output.csv";
        if (args.length > 0) {
            dataFileName = args[0];
        } else {
            throw new RuntimeException("Data file name must be given as first argument");
        }
        if (args.length > 1) {
            outputFileName = args[1];
        }
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("csvfilespout", new CsvFileSourceSpout(dataFileName));

        builder.setBolt("datafilter-bolt", new DataFilterBolt())
                .shuffleGrouping("csvfilespout");

        builder.setBolt("endtime-15min-windowing", new Endtime15minWindowingBolt()
                .withWindow(BaseWindowedBolt.Duration.minutes(15), BaseWindowedBolt.Count.of(1))
                .withTimestampExtractor(new DropoffTimeExtractor())
                .withLag(BaseWindowedBolt.Duration.minutes(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.seconds(1)))
                .shuffleGrouping("datafilter-bolt");

        builder.setBolt("endtime-30min-windowing", new Endtime30minWindowingBolt()
                .withWindow(BaseWindowedBolt.Duration.minutes(30), BaseWindowedBolt.Count.of(1))
                .withTimestampExtractor(new DropoffTimeExtractor())
                .withLag(BaseWindowedBolt.Duration.minutes(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.seconds(1)))
                .shuffleGrouping("datafilter-bolt");

        JoinBolt joinBolt = new JoinBolt("endtime-15min-windowing", GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue())
                .join("endtime-30min-windowing", GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue(), "endtime-15min-windowing")
                .select("window_endtimestamp,starting_cells,median_profits,ending_cells,number_of_taxis,pickup_datetime,dropoff_datetime,processing_starttime")
                .withTumblingWindow(BaseWindowedBolt.Duration.seconds(10));

        builder.setBolt("joiner", joinBolt, 1)
                .fieldsGrouping("endtime-15min-windowing", new Fields(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue()))
                .fieldsGrouping("endtime-30min-windowing", new Fields(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue()));

        builder.setBolt("aggregator", new AggregatorBolt())
                .shuffleGrouping("joiner");

        builder.setBolt("csvwriter", new CsvFileOutputBolt(outputFileName, true))
                .shuffleGrouping("aggregator");

        conf.setDebug(true);

        String topologyName = "profitable-areas";

        conf.setNumWorkers(5);

        return submit(topologyName, conf, builder);
    }
}
