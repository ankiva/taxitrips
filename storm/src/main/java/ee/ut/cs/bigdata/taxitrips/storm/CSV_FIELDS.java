package ee.ut.cs.bigdata.taxitrips.storm;

public enum CSV_FIELDS {
    MEDALLION,
    HACK_LICENSE,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    TRIP_TIME_IN_SECS,
    TRIP_DISTANCE,
    PICKUP_LONGITUDE,
    PICKUP_LATITUDE,
    DROPOFF_LONGITUDE,
    DROPOFF_LATITUDE,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    SURCHARGE,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    TOTAL_AMOUNT,
    ;

    public String getValue(){
        return this.name().toLowerCase();
    }
//    private final String name;
//
//    CSV_FIELDS(String name){
//        this.name = name;
//    }
}
