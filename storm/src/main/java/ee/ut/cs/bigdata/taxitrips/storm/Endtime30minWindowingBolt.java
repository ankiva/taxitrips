package ee.ut.cs.bigdata.taxitrips.storm;

import ee.ut.cs.bigdata.taxitrips.CellCalculator;
import ee.ut.cs.bigdata.taxitrips.Point;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class Endtime30minWindowingBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime30minWindowingBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        LOG.info("execute windowing: "
                + LocalDateTime.ofEpochSecond(inputWindow.getStartTimestamp(), 0, ZoneOffset.UTC) + " - "
                + LocalDateTime.ofEpochSecond(inputWindow.getEndTimestamp(), 0, ZoneOffset.UTC) + "="
                + (inputWindow.getEndTimestamp() - inputWindow.getStartTimestamp()) + ", removed: "
                + inputWindow.getExpired().size() + ", added: " + inputWindow.getNew().size()
                + ", size=" + inputWindow.get().size());
//        Map<Point, Map<String, LocalDateTime>> countsPerEndingCell = new HashMap<>();
        Map<String, ImmutablePair<Point, LocalDateTime>> taxisLastRides = new HashMap<>();
        for (Tuple record : inputWindow.get()) {
            Point endingCell = CellCalculator.calculateQuery2Cell(
                    new BigDecimal(record.getStringByField(CSV_FIELDS.DROPOFF_LATITUDE.getValue())),
                    new BigDecimal(record.getStringByField(CSV_FIELDS.DROPOFF_LONGITUDE.getValue())));
            if (endingCell != null) {
                String medallion = record.getStringByField(CSV_FIELDS.MEDALLION.getValue());
                String rawDropoffDatetime = record.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue());
                if (rawDropoffDatetime != null) {
                    LocalDateTime dropoffDatetime = TaxiDatetimeFormatter.parseDatetime(rawDropoffDatetime);
                    ImmutablePair<Point, LocalDateTime> newPair = new ImmutablePair<>(endingCell, dropoffDatetime);
                    //persist only last rides per taxi
                    taxisLastRides.merge(medallion, newPair,
                            (existingRidePerTaxi, newValue) -> existingRidePerTaxi.getRight().isBefore(dropoffDatetime) ? newValue : existingRidePerTaxi);
//                    ImmutablePair<Point, LocalDateTime> existingRidePerTaxi = taxisLastRides.get(medallion);
//                    if(existingRidePerTaxi != null && existingRidePerTaxi.getRight().isBefore(dropoffDatetime)){
//                        taxisLastRides.put(medallion, new ImmutablePair<>(endingCell, dropoffDatetime));
//                    }
                }
            }
        }
        LOG.info("grouped by last taxi rides: " + taxisLastRides);
        calculateAndEmitTaxiCounts(inputWindow.getEndTimestamp(), taxisLastRides);
    }

    private void calculateAndEmitTaxiCounts(long endTimestamp, Map<String, ImmutablePair<Point, LocalDateTime>> taxisLastRides){
        Map<Point, Set<String>> taxisPerEndingCell = new HashMap<>();
        for(Map.Entry<String, ImmutablePair<Point, LocalDateTime>> entry : taxisLastRides.entrySet()){
            Set<String> taxis = taxisPerEndingCell.get(entry.getValue().getLeft());
            if(taxis == null){
                taxis = new HashSet<>();
                taxisPerEndingCell.put(entry.getValue().getLeft(), taxis);
            }
            taxis.add(entry.getKey());
        }
        emitTaxiCounts(endTimestamp, taxisPerEndingCell);
    }

    private void emitTaxiCounts(long endTimestamp, Map<Point, Set<String>> taxisPerEndingCell){
        for(Map.Entry<Point, Set<String>> entry : taxisPerEndingCell.entrySet()){
            collector.emit(Arrays.asList(endTimestamp + ":" + entry.getKey().toString(), entry.getValue().size()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window_endtimestamp_ending_cell", "number_of_taxis"));
    }
}
