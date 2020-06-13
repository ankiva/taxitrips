package ee.ut.cs.bigdata.taxitrips.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvFileSourceSpout implements IRichSpout {

    private final String dataFileName;
    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;
    private boolean hasHeader = true;
    private boolean headerRead;

    CsvFileSourceSpout(String dataFileName) {
        this.dataFileName = dataFileName;
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
                    collector.emit(Arrays.asList((Object[]) line.split(",")));
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
        return new Fields(Arrays.asList(CSV_FIELDS.values()).stream().map(enumField -> enumField.getValue()).collect(Collectors.toList()));
    }
}
