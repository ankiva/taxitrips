package ee.ut.cs.bigdata.taxitrips.storm;

import ee.ut.cs.bigdata.taxitrips.storm.stripes.ProfitableAreasTopoBuilder;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

public class LocalClusterExec {
    public static void main(String[] args) throws Exception {
        String dataFileName;
        String outputFilename = "output.csv";
        if (args.length > 0) {
            dataFileName = args[0];
        } else {
            throw new RuntimeException("Data file name must be given as first argument");
        }
//        LocalCluster.Builder bilder = new LocalCluster.Builder().build();
        try (LocalCluster cluster = new LocalCluster.Builder().build()) {

            ProfitableAreasTopoBuilder builder = new ProfitableAreasTopoBuilder(dataFileName, outputFilename);
            StormTopology stormTopology = builder.createBuilder().createTopology();
            Config conf = new Config();

            conf.setNumWorkers(5);
            conf.setDebug(true);
            LocalCluster.LocalTopology topo = cluster.submitTopology("profitable-areas", conf, stormTopology);
            Thread.sleep(120000);
            topo.close();
        }
    }
}
