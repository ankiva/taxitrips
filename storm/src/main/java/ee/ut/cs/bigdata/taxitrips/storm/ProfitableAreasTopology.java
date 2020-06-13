package ee.ut.cs.bigdata.taxitrips.storm;

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
    protected int run(String[] args) throws Exception {
        String dataFileName = null;
        if (args.length > 0) {
            dataFileName = args[0];
        } else {
            throw new RuntimeException("Data file name must be given as first argument");
        }
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("csvfilespout", new CsvFileSourceSpout(dataFileName));

        builder.setBolt("endtime-15min-windowing", new Endtime15minWindowingBolt()
                .withWindow(BaseWindowedBolt.Duration.seconds(15), BaseWindowedBolt.Count.of(1))
                .withTimestampExtractor(new DropoffTimeExtractor())
                .withLag(BaseWindowedBolt.Duration.minutes(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.minutes(0)))
                .shuffleGrouping("csvfilespout");

        builder.setBolt("endtime-30min-windowing", new Endtime30minWindowingBolt()
                .withWindow(BaseWindowedBolt.Duration.seconds(30), BaseWindowedBolt.Count.of(1))
                .withTimestampExtractor(new DropoffTimeExtractor())
                .withLag(BaseWindowedBolt.Duration.minutes(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.minutes(0)))
                .shuffleGrouping("csvfilespout");

        JoinBolt joinBolt = new JoinBolt("endtime-15min-windowing", "window_endtimestamp_starting_cell")
                .leftJoin("endtime-30min-windowing", "window_endtimestamp_ending_cell", "endtime-15min-windowing")
                .select("window_endtimestamp_starting_cell,median_profit,number_of_taxis,window_endtimestamp, cell_id")
                .withTumblingWindow(BaseWindowedBolt.Duration.seconds(10));

        builder.setBolt("joiner", joinBolt, 1)
                .fieldsGrouping("endtime-15min-windowing", new Fields("window_endtimestamp_starting_cell"))
                .fieldsGrouping("endtime-30min-windowing", new Fields("window_endtimestamp_ending_cell"));

        //todo
        builder.setBolt("join-back-together-windowing", new ProfitCalculatorWindowingBolt()
                .withWindow(BaseWindowedBolt.Duration.seconds(1))
                .withLag(BaseWindowedBolt.Duration.seconds(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.seconds(0))
                .withTimestampExtractor(new AggregatedTimestampCellIdExtractor()))
                .shuffleGrouping("joiner");

//        builder.setBolt("sout-printing", new SOutBolt(), 1).shuffleGrouping("endtime-15min-windowing");
//        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

        conf.setDebug(true);

        String topologyName = "profitable-areas";

        conf.setNumWorkers(5);

//        if (args != null && args.length > 0) {
//            topologyName = args[0];
//        }
        return submit(topologyName, conf, builder);
    }
}
