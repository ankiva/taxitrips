package ee.ut.cs.bigdata.taxitrips.storm;

import ee.ut.cs.bigdata.taxitrips.storm.util.TupleDataUtil;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DropoffTimeExtractor implements TimestampExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(DropoffTimeExtractor.class);

    @Override
    public long extractTimestamp(Tuple tuple) {
        String dateTime = TupleDataUtil.getDropoffDatetimeString(tuple);
        if (InputDataValidator.validateField(dateTime)) {
            LocalDateTime localDateTime = TaxiDatetimeFormatter.parseDatetime(dateTime);
            if (localDateTime != null) {
                return localDateTime.toEpochSecond(ZoneOffset.UTC); //probably is not utc, but should not matter
            }
        }
        return -1;
    }
}
