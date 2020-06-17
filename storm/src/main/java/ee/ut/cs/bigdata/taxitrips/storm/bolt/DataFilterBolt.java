package ee.ut.cs.bigdata.taxitrips.storm.bolt;

import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.DropoffTimeExtractor;
import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.InputDataValidator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataFilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private DropoffTimeExtractor dropoffTimeExtractor;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.dropoffTimeExtractor = new DropoffTimeExtractor();
    }

    @Override
    public void execute(Tuple input) {
        if (dropoffTimeExtractor.extractTimestamp(input) > 0
                && InputDataValidator.validateField(input.getStringByField(CSV_FIELDS.MEDALLION.getValue()))
                && InputDataValidator.validateField(input.getStringByField(CSV_FIELDS.PICKUP_LATITUDE.getValue()))
                && InputDataValidator.validateField(input.getStringByField(CSV_FIELDS.PICKUP_LONGITUDE.getValue()))
                && InputDataValidator.validateField(input.getStringByField(CSV_FIELDS.DROPOFF_LATITUDE.getValue()))
                && InputDataValidator.validateField(input.getStringByField(CSV_FIELDS.DROPOFF_LONGITUDE.getValue()))) {

            collector.emit(input.getValues());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fieldNames = Arrays.stream(CSV_FIELDS.values())
                .map(CSV_FIELDS::getValue).collect(Collectors.toList());
        fieldNames.add(GEN_FIELDS.PROCESSING_STARTTIME.getValue());
        declarer.declare(new Fields(fieldNames));
    }
}
