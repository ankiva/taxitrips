package ee.ut.cs.bigdata.taxitrips.storm.bolt;

import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.GEN_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.TaxiDatetimeFormatter;
import ee.ut.cs.bigdata.taxitrips.storm.util.ChangeTupleData;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public abstract class AbstractBaseWindowedBolt extends BaseWindowedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBaseWindowedBolt.class);

    protected void printWindowInfo(TupleWindow inputWindow){
        LOG.debug("execute windowing: "
                + LocalDateTime.ofEpochSecond(inputWindow.getStartTimestamp(), 0, ZoneOffset.UTC) + " - "
                + LocalDateTime.ofEpochSecond(inputWindow.getEndTimestamp(), 0, ZoneOffset.UTC) + "="
                + (inputWindow.getEndTimestamp() - inputWindow.getStartTimestamp()) + ", removed: "
                + inputWindow.getExpired().size() + ", added: " + inputWindow.getNew().size()
                + ", size=" + inputWindow.get().size());
    }

    protected void issueWarningIfMultipleChanges(List<Tuple> expiredOnes, List<Tuple> newOnes) {
        int expiredChange = 0;
        int newChange = 0;
        if (expiredOnes != null) {
            expiredChange = expiredOnes.size();
        }
        if (newOnes != null) {
            newChange = newOnes.size();
        }
        if (expiredChange + newChange < 1) {
            LOG.error("no changes for window");
        } else if (expiredChange + newChange > 1) {
            LOG.warn("changed tuples more than 1: expired=" + expiredChange + ", new=" + newChange);
            LOG.warn("expired: " + expiredOnes);
            LOG.warn("new: " + newOnes);
        }
    }

    protected ChangeTupleData selectTupleChanges(List<Tuple> newOnes, List<Tuple> expiredOnes, java.time.Duration windowLength) {
        Tuple tuple;
        if (newOnes != null && !newOnes.isEmpty()) {
            tuple = newOnes.get(0);
            return new ChangeTupleData(tuple.getStringByField(CSV_FIELDS.PICKUP_DATETIME.getValue()),
                    tuple.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue()),
                    tuple.getLongByField(GEN_FIELDS.PROCESSING_STARTTIME.getValue()));
        } else if (expiredOnes != null && !expiredOnes.isEmpty()) {
            tuple = expiredOnes.get(0);
            String rawPickupDatetime = tuple.getStringByField(CSV_FIELDS.PICKUP_DATETIME.getValue());
            String rawDropoffDatetime = tuple.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue());
            Long processingStarttime = tuple.getLongByField(GEN_FIELDS.PROCESSING_STARTTIME.getValue());

            LocalDateTime pickupDatetime = TaxiDatetimeFormatter.parseDatetime(rawPickupDatetime);
            if (pickupDatetime != null) {
                pickupDatetime = pickupDatetime.plus(windowLength);
                rawPickupDatetime = TaxiDatetimeFormatter.formatDatetime(pickupDatetime);
            }
            LocalDateTime dropoffDatetime = TaxiDatetimeFormatter.parseDatetime(rawDropoffDatetime);
            if (dropoffDatetime != null) {
                dropoffDatetime = dropoffDatetime.plus(windowLength);
                rawDropoffDatetime = TaxiDatetimeFormatter.formatDatetime(dropoffDatetime);
            }
            return new ChangeTupleData(rawPickupDatetime, rawDropoffDatetime, processingStarttime);
        } else {
            LOG.debug("No tuple window change - no new tuples and no expired tuples");
        }
        return null;
    }
}
