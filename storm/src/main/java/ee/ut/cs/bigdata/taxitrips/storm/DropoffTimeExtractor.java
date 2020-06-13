package ee.ut.cs.bigdata.taxitrips.storm;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

import java.time.ZoneOffset;

public class DropoffTimeExtractor implements TimestampExtractor {

    @Override
    public long extractTimestamp(Tuple tuple) {
        return TaxiDatetimeFormatter.parseDatetime(tuple.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue()))
                .toEpochSecond(ZoneOffset.UTC); //probably is not utc, but should not matter
    }
}
