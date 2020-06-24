package ee.ut.cs.bigdata.taxitrips.query1;

import ee.ut.cs.bigdata.taxitrips.query1.bolt.End30minWindow;
import ee.ut.cs.bigdata.taxitrips.storm.DropoffTimeExtractor;
import ee.ut.cs.bigdata.taxitrips.storm.bolt.CsvFileOutputBolt;
import ee.ut.cs.bigdata.taxitrips.storm.bolt.DataFilterBolt;
import ee.ut.cs.bigdata.taxitrips.storm.spout.CsvFileSourceSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class FrequentRoutesTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new FrequentRoutesTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        String dataFileName;
        String outputFileName = "Query1Output.csv";
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

        builder.setBolt("endtime-30min-windowing", new End30minWindow()
                .withWindow(BaseWindowedBolt.Duration.minutes(30)/*, BaseWindowedBolt.Count.of(1)*/)
                .withTimestampExtractor(new DropoffTimeExtractor())
                .withLag(BaseWindowedBolt.Duration.minutes(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.seconds(1)))
                .shuffleGrouping("datafilter-bolt");


        builder.setBolt("csvwriter", new CsvFileOutputBolt(outputFileName, true))
                .shuffleGrouping("endtime-30min-windowing");

//        conf.setDebug(true);

        String topologyName = "frequent-routes";

        conf.setNumWorkers(5);

        return submit(topologyName, conf, builder);
    }
}
