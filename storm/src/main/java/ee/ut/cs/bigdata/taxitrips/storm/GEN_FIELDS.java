package ee.ut.cs.bigdata.taxitrips.storm;

public enum GEN_FIELDS {
    WINDOW_ENDTIMESTAMP,
    PROCESSING_STARTTIME,
    STARTING_CELLS,
    ENDING_CELLS,
    NUMBER_OF_TAXIS,
    MEDIAN_PROFITS,
    ;

    public String getValue(){
        return this.name().toLowerCase();
    }
}
