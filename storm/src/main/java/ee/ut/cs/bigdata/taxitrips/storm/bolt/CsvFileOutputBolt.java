package ee.ut.cs.bigdata.taxitrips.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class CsvFileOutputBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CsvFileOutputBolt.class);

    private final String dataFileName;
    private BufferedWriter bufferedWriter;
    private final boolean printHeader;
    private boolean headerPrinted = false;

    public CsvFileOutputBolt(String dataFileName) {
        this(dataFileName, false);
    }

    public CsvFileOutputBolt(String dataFileName, boolean printHeader) {
        this.dataFileName = dataFileName;
        this.printHeader = printHeader;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        try {
            this.bufferedWriter = new BufferedWriter(new FileWriter(this.dataFileName, false));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        handleHeader(input);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object o : input.getValues()) {
            if (first) {
                sb.append(o);
                first = false;
            } else {
                sb.append(",").append(o);
            }
        }
        try {
            this.bufferedWriter.write(sb.toString());
            this.bufferedWriter.newLine();
        } catch (IOException e) {
            LOG.error("Failed to write to csv output field", e);
        }
    }

    private void handleHeader(Tuple input) {
        if (printHeader && !headerPrinted) {
            headerPrinted = true;
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String field : input.getFields().toList()) {
                if (first) {
                    sb.append(field);
                    first = false;
                } else {
                    sb.append(",").append(field);
                }
            }

            try {
                this.bufferedWriter.write(sb.toString());
                this.bufferedWriter.newLine();
            } catch (IOException e) {
                LOG.error("Failed to write to csv output field", e);
            }
        }
    }

    @Override
    public void cleanup() {
        try {
            this.bufferedWriter.flush();
        } catch (IOException e) {
            LOG.info("Failed to flush", e);
        }
        try {
            this.bufferedWriter.close();
        } catch (IOException e) {
            LOG.info("Failed to close", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
