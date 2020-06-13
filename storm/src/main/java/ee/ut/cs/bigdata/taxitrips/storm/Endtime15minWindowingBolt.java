package ee.ut.cs.bigdata.taxitrips.storm;

import ee.ut.cs.bigdata.taxitrips.CellCalculator;
import ee.ut.cs.bigdata.taxitrips.Point;
import org.apache.commons.collections.ComparatorUtils;
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

public class Endtime15minWindowingBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Endtime15minWindowingBolt.class);

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
        Map<Point, List<BigDecimal>> profitsPerStartingCell = new HashMap<>();
        for (Tuple record : inputWindow.get()) {
            Point startingCell = CellCalculator.calculateQuery2Cell(new BigDecimal(record.getStringByField(CSV_FIELDS.PICKUP_LATITUDE.getValue())),
                    new BigDecimal(record.getStringByField(CSV_FIELDS.PICKUP_LONGITUDE.getValue())));
            if (startingCell != null) {
                BigDecimal fare = new BigDecimal(record.getStringByField(CSV_FIELDS.FARE_AMOUNT.getValue()));
                BigDecimal tip = new BigDecimal(record.getStringByField(CSV_FIELDS.TIP_AMOUNT.getValue()));
                BigDecimal profit = fare.add(tip);
                List<BigDecimal> cellProfits = profitsPerStartingCell.get(startingCell);
                if (cellProfits == null) {
                    cellProfits = new ArrayList<>();
                    profitsPerStartingCell.put(startingCell, cellProfits);
                }
                cellProfits.add(profit);
            }
        }
        LOG.info("grouped by starting cell: " + profitsPerStartingCell);
        calculateAndEmitProfitMedians(inputWindow.getEndTimestamp(), profitsPerStartingCell);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window_endtimestamp_starting_cell", "median_profit", "window_endtimestamp", "cell_id"));
    }

    private void calculateAndEmitProfitMedians(long endTimestamp, Map<Point, List<BigDecimal>> profitPerStartingCell) {
        for (Map.Entry<Point, List<BigDecimal>> entry : profitPerStartingCell.entrySet()) {
            List<BigDecimal> profits = entry.getValue();
            BigDecimal profit;
            if (profits.size() > 1) {
                profits.sort(ComparatorUtils.naturalComparator());
                profit = profits.get(profits.size() / 2);
            } else { //size==1
                profit = profits.get(0);
            }
            //TODO emit all starting cells as list for one endtimestamp?
//            LOG.info("emitting " + endTimestamp + " " + entry.getKey().toString() + " " + profit.toPlainString());
            collector.emit(Arrays.asList(endTimestamp + ":" + entry.getKey().toString(), profit.toPlainString(), endTimestamp, entry.getKey().toString()));
        }
    }
}
