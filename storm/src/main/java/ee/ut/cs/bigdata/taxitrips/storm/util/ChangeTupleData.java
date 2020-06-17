package ee.ut.cs.bigdata.taxitrips.storm.util;

public class ChangeTupleData {
    public final String pickupDatetime;
    public final String dropoffDatetime;
    public final Long processingStarttime;

    public ChangeTupleData(String pickupDatetime, String dropoffDatetime, Long processingStarttime){
        this.pickupDatetime = pickupDatetime;
        this.dropoffDatetime = dropoffDatetime;
        this.processingStarttime = processingStarttime;
    }
}
