package ee.ut.cs.bigdata.taxitrips.storm;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

public class AggregatedTimestampCellIdExtractor implements TimestampExtractor {
    @Override
    public long extractTimestamp(Tuple tuple) {
        return tuple.getLongByField(GEN_FIELDS.WINDOW_ENDTIMESTAMP.getValue());
    }
}
