package ee.ut.cs.bigdata.taxitrips.storm.util;

import ee.ut.cs.bigdata.taxitrips.storm.CSV_FIELDS;
import ee.ut.cs.bigdata.taxitrips.storm.InputDataValidator;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;

public class TupleDataUtil {

    public static BigDecimal getPickupLatitude(Tuple record){
        return InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.PICKUP_LATITUDE.getValue()));
    }

    public static BigDecimal getPickupLongitude(Tuple record){
        return InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.PICKUP_LONGITUDE.getValue()));
    }

    public static BigDecimal getFareAmount(Tuple record){
        return InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.FARE_AMOUNT.getValue()));
    }

    public static BigDecimal getTipAmount(Tuple record){
        return InputDataValidator.parseBigDecimal(record.getStringByField(CSV_FIELDS.TIP_AMOUNT.getValue()));
    }

    public static String getDropoffDatetimeString(Tuple record){
        return record.getStringByField(CSV_FIELDS.DROPOFF_DATETIME.getValue());
    }

    public static String getPickupDatetimeString(Tuple record){
        return record.getStringByField(CSV_FIELDS.PICKUP_DATETIME.getValue());
    }
}
