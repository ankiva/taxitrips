package ee.ut.cs.bigdata.taxitrips.storm.stripes;

import ee.ut.cs.bigdata.taxitrips.Cell;
import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.util.ImmutablePair;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Endtime30minWindowingBolt extends ee.ut.cs.bigdata.taxitrips.storm.bolt.Endtime30minWindowingBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime30minWindowingBolt.class);

    @Override
    protected void emitTaxiCounts(long endTimestamp, Map<Cell, Set<String>> taxisPerEndingCell, String pickupDatetime, String dropoffDatetime, Long processingStarttime) {
        if (!taxisPerEndingCell.isEmpty()) {
            List<Object> output = new ArrayList<>();
            output.add(endTimestamp);
            ImmutablePair<String, String> joins = joinCellIdsAndNumberOfTaxies(taxisPerEndingCell);
            output.add(joins.getLeft());
            output.add(joins.getRight());
            output.add(pickupDatetime);
            output.add(dropoffDatetime);
            output.add(processingStarttime);
            getCollector().emit(output);
        }
    }

    private ImmutablePair<String, String> joinCellIdsAndNumberOfTaxies(Map<Cell, Set<String>> taxisPerEndingCell) {
        StringBuilder cellIds = new StringBuilder();
        StringBuilder numberOfTaxis = new StringBuilder();
        boolean first = true;
        for (Map.Entry<Cell, Set<String>> entry : taxisPerEndingCell.entrySet()) {
            if (first) {
                first = false;
            } else {
                cellIds.append(",");
                numberOfTaxis.append(",");
            }
            cellIds.append(entry.getKey().toString());
            numberOfTaxis.append(entry.getValue().size());
        }
        return new ImmutablePair<>(cellIds.toString(), numberOfTaxis.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue(), GEN_FIELDS.ENDING_CELLS.getValue(), GEN_FIELDS.NUMBER_OF_TAXIS.getValue(),
                CSV_FIELDS.PICKUP_DATETIME.getValue(), CSV_FIELDS.DROPOFF_DATETIME.getValue(), GEN_FIELDS.PROCESSING_STARTTIME.getValue()));
    }
}
