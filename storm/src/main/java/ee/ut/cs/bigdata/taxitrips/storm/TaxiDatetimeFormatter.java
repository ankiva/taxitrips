package ee.ut.cs.bigdata.taxitrips.storm;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TaxiDatetimeFormatter {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime parseDatetime(String dateTime) {
        return LocalDateTime.from(DATE_TIME_FORMATTER.parse(dateTime));
    }
}
