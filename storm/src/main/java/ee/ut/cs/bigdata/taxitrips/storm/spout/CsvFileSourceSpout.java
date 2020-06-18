package ee.ut.cs.bigdata.taxitrips.storm.spout;

import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvFileSourceSpout implements IRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(CsvFileSourceSpout.class);

    private final String dataFileName;
    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;
    private final boolean hasHeader;
    private boolean headerRead;

    public CsvFileSourceSpout(String dataFileName) {
        this.dataFileName = dataFileName;
        this.hasHeader = true;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.bufferedReader = new BufferedReader(new FileReader(this.dataFileName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to open file", e);
        }
    }

    @Override
    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            System.err.println("Err during file close");
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        try {
            String line = bufferedReader.readLine();
            if (line != null) {
                if (hasHeader && !headerRead) {
                    //read in header
                    headerRead = true;
                } else {
                    String[] splitValues = line.split(",");
                    List<Object> values = new ArrayList<>();
                    for(String value : splitValues){
                        values.add(value);
                    }
                    values.add(System.currentTimeMillis());
                    collector.emit(values);
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to read file");
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(generateFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private Fields generateFields() {
        List<String> fieldNames = Arrays.stream(CSV_FIELDS.values()).map(CSV_FIELDS::getValue).collect(Collectors.toList());
        fieldNames.add(GEN_FIELDS.PROCESSING_STARTTIME.getValue());
        LOG.info("csv spout field names: " + fieldNames);
        return new Fields(fieldNames);
    }
}
