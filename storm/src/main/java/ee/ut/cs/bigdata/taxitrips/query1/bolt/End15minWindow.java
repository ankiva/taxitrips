package ee.ut.cs.bigdata.taxitrips.query1.bolt;

import ee.ut.cs.bigdata.taxitrips.Cell;
import ee.ut.cs.bigdata.taxitrips.CellCalculator;
import ee.ut.cs.bigdata.taxitrips.storm.bolt.AbstractBaseWindowedBolt;
import ee.ut.cs.bigdata.taxitrips.storm.util.ChangeTupleData;
import ee.ut.cs.bigdata.taxitrips.storm.util.TupleDataUtil;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class End15minWindow extends AbstractBaseWindowedBolt {


    private static final Logger LOG = LoggerFactory.getLogger(End15minWindow.class);

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
            List<Pair<String, String>> routes = new ArrayList<>();
            for (Tuple record : inputWindow.get()) {
                Cell startingCell = calculateStartingCell(record);
                Cell endingCell = calculateEndingCell(record);

                if (startingCell != null && endingCell != null) {
                    Pair<String, String> route = new Pair<>(startingCell.toString(), endingCell.toString());
                    routes.add(route);
                }
            }
            calculateAndEmitRouteFrequency(inputWindow.getEndTimestamp(), routes, inputWindow.getExpired(), inputWindow.getNew());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("starting_cell", "ending_cell", "route_freq", "window_endtimestamp", "pickup_datetime",
                "dropoff_datetime", "processing_starttime"));
    }

    private Cell calculateStartingCell(Tuple record) {
        BigDecimal latitude = TupleDataUtil.getPickupLatitude(record);
        BigDecimal longitude = TupleDataUtil.getPickupLongitude(record);
        if (latitude != null && longitude != null) {
            return CellCalculator.calculateQuery1Cell(latitude, longitude);
        }
        return null;
    }

    private Cell calculateEndingCell(Tuple record) {
        BigDecimal latitude = TupleDataUtil.getDropoffLatitude(record);
        BigDecimal longitude = TupleDataUtil.getDropoffLongitude(record);
        if (latitude != null && longitude != null) {
            return CellCalculator.calculateQuery1Cell(latitude, longitude);
        }
        return null;
    }

    private void calculateAndEmitRouteFrequency(long endTimestamp, List<Pair<String, String>> routes, List<Tuple> expiredOnes, List<Tuple> newOnes) {
        // Map all routes and count their frequency
        Map<Pair<String, String>, Integer> routeMap = new HashMap<>();
        for (Pair<String, String> route : routes) {
            if (routeMap.containsKey(route)) {
                int value = routeMap.get(route);
                routeMap.put(route, value + 1);
            } else {
                routeMap.put(route, 1);
            }
        }
        // Make a list of route frequency corresponding to each entry
        List<Integer> counts = new ArrayList<>();
        for (Pair<String, String> route : routes) {
            int value = routeMap.get(route);
            counts.add(value);
        }
        emitForOneChange(endTimestamp, routes, counts, expiredOnes, newOnes);
    }
    //todo redesign
    private void emitForOneChange(long endTimestamp, List<Pair<String, String>> routes, List<Integer> counts, List<Tuple> expiredOnes, List<Tuple> newOnes) {
        issueWarningIfMultipleChanges(expiredOnes, newOnes);

        ChangeTupleData changeTupleData = selectTupleChanges(newOnes, expiredOnes, java.time.Duration.ofMinutes(15));
        String pickupDatetime = null;
        String dropoffDatetime = null;
        Long processingStarttime = null;
        if (changeTupleData != null) {
            pickupDatetime = changeTupleData.pickupDatetime;
            dropoffDatetime = changeTupleData.dropoffDatetime;
            processingStarttime = changeTupleData.processingStarttime;
        }

        emitCellsAndRouteFreq(endTimestamp, pickupDatetime, dropoffDatetime, processingStarttime, routes, counts);
    }


    protected void emitCellsAndRouteFreq(long endTimestamp, String pickupDatetime, String dropoffDatetime,
                                         Long processingStarttime, List<Pair<String, String>> routes, List<Integer> counts) {
        for (int i = 0; i < routes.size(); i++) {
            collector.emit(Arrays.asList(routes.get(i).getKey(), routes.get(i).getValue(), counts.get(i), endTimestamp,
                    pickupDatetime, dropoffDatetime, processingStarttime));
        }
    }

}
