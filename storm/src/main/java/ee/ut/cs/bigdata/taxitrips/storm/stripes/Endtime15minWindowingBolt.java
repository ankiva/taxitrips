package ee.ut.cs.bigdata.taxitrips.storm.stripes;

import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.util.ImmutablePair;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Endtime15minWindowingBolt extends ee.ut.cs.bigdata.taxitrips.storm.bolt.Endtime15minWindowingBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime15minWindowingBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue(), GEN_FIELDS.STARTING_CELLS.getValue(), GEN_FIELDS.MEDIAN_PROFITS.getValue(),
                CSV_FIELDS.PICKUP_DATETIME.getValue(), CSV_FIELDS.DROPOFF_DATETIME.getValue(), GEN_FIELDS.PROCESSING_STARTTIME.getValue()));
    }

    @Override
    protected void emitProfitMedians(long endTimestamp, String pickupDatetime, String dropoffDatetime, Long processingStarttime, List<ImmutablePair<String, String>> profitMedians) {
        if (!profitMedians.isEmpty()) {
            List<Object> output = new ArrayList<>();
            output.add(endTimestamp);
            ImmutablePair<String,String> joins = joinCellsAndProfitMedians(profitMedians);
            output.add(joins.getLeft());
            output.add(joins.getRight());
            output.add(pickupDatetime);
            output.add(dropoffDatetime);
            output.add(processingStarttime);
            getCollector().emit(output);

        }
    }

    private ImmutablePair<String, String> joinCellsAndProfitMedians(List<ImmutablePair<String, String>> profitMedians){
        StringBuilder cells = new StringBuilder();
        StringBuilder profitMediansCollected = new StringBuilder();
        boolean first = true;
        for (ImmutablePair<String,String> pair : profitMedians) {
            if (first) {
                first = false;
            } else {
                cells.append(",");
                profitMediansCollected.append(",");
            }
            cells.append(pair.getLeft());
            profitMediansCollected.append(pair.getRight());
        }
        return new ImmutablePair<>(cells.toString(), profitMediansCollected.toString());
    }
}
