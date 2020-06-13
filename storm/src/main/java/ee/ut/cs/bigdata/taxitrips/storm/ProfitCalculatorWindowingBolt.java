package ee.ut.cs.bigdata.taxitrips.storm;

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
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ProfitCalculatorWindowingBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ProfitCalculatorWindowingBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<CompleteOutputRecord> outputRecords = new ArrayList<>();
        for(Tuple record : inputWindow.get()){
            BigDecimal medianProfit = new BigDecimal(record.getStringByField("median_profit"));
            Integer numberOfTaxis = record.getIntegerByField("number_of_taxis");
            BigDecimal profitability = medianProfit.divide(BigDecimal.valueOf(Optional.ofNullable(numberOfTaxis).orElse(1)), RoundingMode.HALF_UP);
            String cellId = record.getStringByField("cell_id");
            CompleteOutputRecord out = new CompleteOutputRecord(cellId, numberOfTaxis, record.getStringByField("median_profit"), profitability);
            outputRecords.add(out);
        }
        outputRecords.sort((outputRecord1, outputRecord2) -> -outputRecord1.profitability.compareTo(outputRecord2.profitability));
        LOG.info("outputRecords: " + outputRecords);
        List<Object> output = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            CompleteOutputRecord outputRecord = null;
            if(i < outputRecords.size()){
                outputRecord = outputRecords.get(i);
            }
            if(outputRecord != null){
                output.add(outputRecord.cellId);
                output.add(outputRecord.emptyTaxies);
                output.add(outputRecord.medianProfit);
                output.add(outputRecord.profitability);
            } else {
                output.add(null);
                output.add(null);
                output.add(null);
                output.add(null);
            }
        }
        collector.emit(output);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<>();
        for(int i = 1; i < 11; i++){
            fields.add("profitable_cell_id_" + i);
            fields.add("empty_taxies_in_cell_id_" + i);
            fields.add("median_profit_in_cell_id_" + i);
            fields.add("profitability_of_cell_" + i);
        }
        declarer.declare(new Fields(fields));
    }
}
