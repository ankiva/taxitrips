package ee.ut.cs.bigdata.taxitrips.storm.bolt;

import ee.ut.cs.bigdata.taxitrips.Cell;
import ee.ut.cs.bigdata.taxitrips.CellCalculator;
import ee.ut.cs.bigdata.taxitrips.storm.util.ChangeTupleData;
import ee.ut.cs.bigdata.taxitrips.storm.util.ImmutablePair;
import ee.ut.cs.bigdata.taxitrips.storm.util.TupleDataUtil;
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

public class Endtime15minWindowingBolt extends AbstractBaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime15minWindowingBolt.class);

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
//        printWindowInfo(inputWindow);
        Map<Cell, List<BigDecimal>> profitsPerStartingCell = new HashMap<>();
        for (Tuple record : inputWindow.get()) {
            Cell startingCell = calculateStartingCell(record);
            if (startingCell != null) {
                BigDecimal profit = calculateRecordProfit(record);
                if (profit != null) {
                    List<BigDecimal> cellProfits = profitsPerStartingCell.get(startingCell);
                    if (cellProfits == null) {
                        cellProfits = new ArrayList<>();
                        profitsPerStartingCell.put(startingCell, cellProfits);
                    }
                    cellProfits.add(profit);
                }
            }
        }
        LOG.debug("grouped by starting cell: " + profitsPerStartingCell);
        calculateAndEmitProfitMedians(inputWindow.getEndTimestamp(), profitsPerStartingCell, inputWindow.getExpired(), inputWindow.getNew());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window_endtimestamp_starting_cell", "median_profit", "window_endtimestamp", "cell_id", "change_pickup_datetime",
                "change_dropoff_datetime", "processing_starttime"));
    }

    private Cell calculateStartingCell(Tuple record) {
        BigDecimal latitude = TupleDataUtil.getPickupLatitude(record);
        BigDecimal longitude = TupleDataUtil.getPickupLongitude(record);
        if (latitude != null && longitude != null) {
            return CellCalculator.calculateQuery2Cell(latitude, longitude);
        }
        return null;
    }

    private BigDecimal calculateRecordProfit(Tuple record) {
        BigDecimal fare = TupleDataUtil.getFareAmount(record);
        BigDecimal tip = TupleDataUtil.getTipAmount(record);
        if (fare != null || tip != null) {
            fare = Optional.ofNullable(fare).orElse(BigDecimal.ZERO);
            tip = Optional.ofNullable(tip).orElse(BigDecimal.ZERO);
            if (fare.signum() >= 0 && tip.signum() >= 0) {
                return fare.add(tip);
            }
        }
        return null;
    }

    private void calculateAndEmitProfitMedians(long endTimestamp, Map<Cell, List<BigDecimal>> profitPerStartingCell, List<Tuple> expiredOnes, List<Tuple> newOnes) {
        List<ImmutablePair<String, String>> profitMedians = new ArrayList<>();
        for (Map.Entry<Cell, List<BigDecimal>> entry : profitPerStartingCell.entrySet()) {
            List<BigDecimal> profits = entry.getValue();
            BigDecimal profit;
            if (profits.size() > 1) {
                profits.sort(Comparator.naturalOrder());
                profit = profits.get(profits.size() / 2);
            } else { //size==1
                profit = profits.get(0);
            }
            profitMedians.add(new ImmutablePair<>(entry.getKey().toString(), profit.toPlainString()));
        }
        emitForOneChange(endTimestamp, profitMedians, expiredOnes, newOnes);
    }

    //todo redesign
    private void emitForOneChange(long endTimestamp, List<ImmutablePair<String, String>> profitMedians, List<Tuple> expiredOnes, List<Tuple> newOnes) {
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

        emitProfitMedians(endTimestamp, pickupDatetime, dropoffDatetime, processingStarttime, profitMedians);
    }

    protected void emitProfitMedians(long endTimestamp, String pickupDatetime, String dropoffDatetime, Long processingStarttime, List<ImmutablePair<String, String>> profitMedians) {
        for (ImmutablePair<String, String> pair : profitMedians) {
            collector.emit(Arrays.asList(endTimestamp + ";" + pair.getLeft(), pair.getRight(), endTimestamp, pair.getLeft(),
                    pickupDatetime, dropoffDatetime, processingStarttime));
        }
    }
}
