package ee.ut.cs.bigdata.taxitrips.storm.stripes;

import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.util.TupleDataUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class AggregatorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private List<Object> latestData;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long endTimestamp = input.getLongByField(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue());
        Long processingStarttime = input.getLongByField(GEN_FIELDS.PROCESSING_STARTTIME.getValue());
        Map<String, OutputCellData> cells = new HashMap<>();
        collectProfitData(input, cells);
        collectEmptyTaxiesData(input, cells);
        List<OutputCellData> orderedCells = filterAndConvertToSortedList(cells);
        emit(endTimestamp, TupleDataUtil.getPickupDatetimeString(input), TupleDataUtil.getDropoffDatetimeString(input), orderedCells, processingStarttime);
    }

    private void collectProfitData(Tuple input, Map<String, OutputCellData> cells) {
        String[] profitCells = input.getStringByField(GEN_FIELDS.STARTING_CELLS.getValue()).split(",");
        String[] profits = input.getStringByField(GEN_FIELDS.MEDIAN_PROFITS.getValue()).split(",");
        for (int i = 0; i < profitCells.length; i++) {
            OutputCellData cellData = new OutputCellData(profitCells[i]);
            cellData.setProfit(profits[i]);
            cellData.setProfitability(new BigDecimal(profits[i]));
            cells.put(cellData.getCellId(), cellData);
        }
    }

    private void collectEmptyTaxiesData(Tuple input, Map<String, OutputCellData> cells) {
        String[] emptyTaxiesCells = input.getStringByField(GEN_FIELDS.ENDING_CELLS.getValue()).split(",");
        String[] emptyTaxies = input.getStringByField(GEN_FIELDS.NUMBER_OF_TAXIS.getValue()).split(",");
        for (int i = 0; i < emptyTaxiesCells.length; i++) {
            OutputCellData cellData = cells.get(emptyTaxiesCells[i]);
            if (cellData != null) {
                String emptyTaxiesCellData = emptyTaxies[i];
                cellData.setEmptyTaxies(emptyTaxiesCellData);
                BigDecimal divisor = BigDecimal.ONE;
                if (emptyTaxiesCellData != null && emptyTaxiesCellData.length() > 0 && !emptyTaxiesCellData.equals("null")) {
                    divisor = new BigDecimal(emptyTaxiesCellData);
                }
                BigDecimal divisible = BigDecimal.ZERO;
                if (cellData.getProfit() != null && !cellData.getProfit().isEmpty() && !cellData.getProfit().equals("null")) {
                    divisible = new BigDecimal(cellData.getProfit());
                }
                cellData.setProfitability(divisible.divide(divisor, RoundingMode.HALF_UP));
            }
        }
    }

    private List<OutputCellData> filterAndConvertToSortedList(Map<String, OutputCellData> cells) {
        List<OutputCellData> cellsList = new ArrayList<>();
        cells.forEach((s, outputCellData) -> {
            if (outputCellData.getEmptyTaxies() != null) cellsList.add(outputCellData);
        });
        cellsList.sort((o1, o2) -> -o1.getProfitability().compareTo(o2.getProfitability()));
        return cellsList;
    }

    private void emit(long endTimestamp, String pickupDatetime, String dropoffDatetime, List<OutputCellData> cells, Long processingStarttime) {
        List<Object> emits = new ArrayList<>();
        emits.add(endTimestamp);
        emits.add(pickupDatetime);
        emits.add(dropoffDatetime);
        for (int i = 0; i < 10; i++) {
            if (i < cells.size()) {
                OutputCellData cellData = cells.get(i);
                emits.add(cellData.getCellId());
                emits.add(cellData.getEmptyTaxies());
                emits.add(cellData.getProfit());
                emits.add(cellData.getProfitability().toPlainString());
            } else {
                emits.add(null);
                emits.add(null);
                emits.add(null);
                emits.add(null);
            }
        }
        if (processingStarttime != null) {
            emits.add(System.currentTimeMillis() - processingStarttime);
        } else {
            emits.add(null);
        }

        if (hasChanged(emits)) {
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<>();
        fields.add("endtimestamp");
        fields.add("pickup_datetime");
        fields.add("dropoff_datetime");
        for (int i = 1; i < 11; i++) {
            fields.add("profitable_cell_id_" + i);
            fields.add("empty_taxies_in_cell_id_" + i);
            fields.add("median_profit_in_cell_id_" + i);
            fields.add("profitability_of_cell_" + i);
        }
        fields.add("delay");
        declarer.declare(new Fields(fields));
    }
}
