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

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class End30minWindowingBolt extends AbstractBaseWindowedBolt {


    private static final Logger LOG = LoggerFactory.getLogger(End30minWindowingBolt.class);

    private OutputCollector collector;

    private List<Object> latestData;

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
                // Calculate starting and ending cell
                Cell startingCell = calculateStartingCell(record);
                Cell endingCell = calculateEndingCell(record);

                if (startingCell != null && endingCell != null) {
                    // Make a route
                    Pair<String, String> route = new Pair<>(startingCell.toString(), endingCell.toString());
                    routes.add(route);
                }
            }
            calculateAndEmitRouteFrequency(inputWindow.getEndTimestamp(), routes, inputWindow.getExpired(), inputWindow.getNew());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<>();
        fields.add("pickup_datetime");
        fields.add("dropoff_datetime");
        for (int i = 1; i < 11; i++) {
            fields.add("start_cell_id_" + i);
            fields.add("end_cell_id_" + i);
        }
        fields.add("delay");
        declarer.declare(new Fields(fields));
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

    private Map<Pair<String, String>, Integer> sortByValues(Map<Pair<String, String>, Integer> routes) {
        // Sort the map by values in Ascending order
        Map<Pair<String, String>, Integer> sorted = routes
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(
                        toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2,
                                LinkedHashMap::new));
        return sorted;
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
        // Sort routes by their frequency
        Map<Pair<String, String>, Integer> sortedRoutes = sortByValues(routeMap);
        // Save routes and their frequencies as lists
        List<Pair<String, String>> cells = new ArrayList<>(sortedRoutes.keySet());
        List<Integer> routeFreq = new ArrayList<>(sortedRoutes.values());

        emitForOneChange(endTimestamp, cells, routeFreq, expiredOnes, newOnes);
    }

    private void emitForOneChange(long endTimestamp, List<Pair<String, String>> routes, List<Integer> counts, List<Tuple> expiredOnes, List<Tuple> newOnes) {
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

        emitCellsAndRouteFreq(pickupDatetime, dropoffDatetime, processingStarttime, routes, counts);
    }


    protected void emitCellsAndRouteFreq(String pickupDatetime, String dropoffDatetime,
                                         Long processingStarttime, List<Pair<String, String>> routes, List<Integer> counts) {
        // Loop over routes in reverse order to get top 10 frequent routes
        List<Object> emits = new ArrayList<>();
        emits.add(pickupDatetime);
        emits.add(dropoffDatetime);
        for(int i = 0; i < 10; i++){
            if(i < routes.size()){
                Pair<String, String> cell = routes.get(routes.size() - i - 1);
                emits.add(cell.getKey());
                emits.add(cell.getValue());
            } else {
                emits.add(null);
                emits.add(null);
            }
        }
        emits.add(System.currentTimeMillis() - processingStarttime);

        if(hasChanged(emits)){
            collector.emit(emits);
            latestData = emits;
        }
    }

    private boolean hasChanged(List<Object> currentData) {
        if (this.latestData != null) {
            for (int i = 3; i < currentData.size() - 1; i++) {
                if (!Objects.equals(currentData.get(i), this.latestData.get(i))) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

}
