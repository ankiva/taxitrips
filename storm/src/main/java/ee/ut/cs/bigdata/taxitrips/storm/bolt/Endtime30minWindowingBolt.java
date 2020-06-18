package ee.ut.cs.bigdata.taxitrips.storm.bolt;

import ee.ut.cs.bigdata.taxitrips.Cell;
import ee.ut.cs.bigdata.taxitrips.CellCalculator;
import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.InputDataValidator;
import ee.ut.cs.bigdata.taxitrips.storm.TaxiDatetimeFormatter;
import ee.ut.cs.bigdata.taxitrips.storm.util.ChangeTupleData;
import ee.ut.cs.bigdata.taxitrips.storm.util.ImmutablePair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

public class Endtime30minWindowingBolt extends AbstractBaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime30minWindowingBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    protected OutputCollector getCollector() {
        return this.collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        printWindowInfo(inputWindow);
        if(inputWindow.getNew().size() + inputWindow.getExpired().size() > 0) {
            Map<String, ImmutablePair<Cell, Instant>> taxisLastRides = new HashMap<>();
            for (Tuple record : inputWindow.get()) {
                BigDecimal latitude = InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.DROPOFF_LATITUDE.getValue()));
                BigDecimal longitude = InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.DROPOFF_LONGITUDE.getValue()));
                if (latitude != null && longitude != null) {
                    Cell endingCell = CellCalculator.calculateQuery2Cell(latitude, longitude);
                    if (endingCell != null) {
                        String medallion = record.getStringByField(CSV_FIELDS.MEDALLION.getValue());
                        String rawDropoffDatetime = record.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue());
                        if (InputDataValidator.validateField(medallion) && InputDataValidator.validateField(rawDropoffDatetime)) {
                            Instant dropoffDatetime = TaxiDatetimeFormatter.parseDatetime(rawDropoffDatetime);
                            if (dropoffDatetime != null) {
                                ImmutablePair<Cell, Instant> newPair = new ImmutablePair<>(endingCell, dropoffDatetime);
                                //persist only last rides per taxi
                                taxisLastRides.merge(medallion, newPair,
                                        (existingRidePerTaxi, newValue) -> existingRidePerTaxi.getRight().isBefore(dropoffDatetime) ? newValue : existingRidePerTaxi);
                            }
                        }
                    }
                }
            }
            LOG.info("grouped by last taxi rides: " + taxisLastRides);
            calculateAndEmitTaxiCounts(inputWindow.getEndTimestamp(), taxisLastRides, inputWindow.getExpired(), inputWindow.getNew());
        }
    }

    private void calculateAndEmitTaxiCounts(long endTimestamp, Map<String, ImmutablePair<Cell, Instant>> taxisLastRides, List<Tuple> expiredOnes, List<Tuple> newOnes) {
        Map<Cell, Set<String>> taxisPerEndingCell = new HashMap<>();
        for (Map.Entry<String, ImmutablePair<Cell, Instant>> entry : taxisLastRides.entrySet()) {
            Set<String> taxis = taxisPerEndingCell.get(entry.getValue().getLeft());
            if (taxis == null) {
                taxis = new HashSet<>();
                taxisPerEndingCell.put(entry.getValue().getLeft(), taxis);
            }
            taxis.add(entry.getKey());
        }
        emitForOneChange(endTimestamp, taxisPerEndingCell, expiredOnes, newOnes);
    }

    private void emitForOneChange(long endTimestamp, Map<Cell, Set<String>> taxisPerEndingCell, List<Tuple> expiredOnes, List<Tuple> newOnes) {
        issueWarningIfMultipleChanges(expiredOnes, newOnes);

        ChangeTupleData changeTupleData = selectTupleChanges(newOnes, expiredOnes, java.time.Duration.ofMinutes(30));
        String pickupDatetime = null;
        String dropoffDatetime = null;
        Long processingStarttime = null;
        if (changeTupleData != null) {
            pickupDatetime = changeTupleData.pickupDatetime;
            dropoffDatetime = changeTupleData.dropoffDatetime;
            processingStarttime = changeTupleData.processingStarttime;
        }

        emitTaxiCounts(endTimestamp, taxisPerEndingCell, pickupDatetime, dropoffDatetime, processingStarttime);
    }

    protected void emitTaxiCounts(long endTimestamp, Map<Cell, Set<String>> taxisPerEndingCell, String pickupDatetime, String dropoffDatetime, Long processingStarttime) {
        for (Map.Entry<Cell, Set<String>> entry : taxisPerEndingCell.entrySet()) {
            collector.emit(Arrays.asList(endTimestamp + ";" + entry.getKey().toString(), endTimestamp, entry.getValue().size(),
                    pickupDatetime, dropoffDatetime, processingStarttime));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window_endtimestamp_ending_cell", "window_endtimestamp", "number_of_taxis", "pickup_datetime", "dropoff_datetime",
                "processing_starttime"));
    }
}
