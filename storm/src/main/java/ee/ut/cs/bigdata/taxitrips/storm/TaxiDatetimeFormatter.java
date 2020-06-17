package ee.ut.cs.bigdata.taxitrips.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class TaxiDatetimeFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(TaxiDatetimeFormatter.class);

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime parseDatetime(String dateTime) {
        try {
            return LocalDateTime.from(DATE_TIME_FORMATTER.parse(dateTime));
        } catch (DateTimeParseException e) {
            LOG.info("Failed to parse date " + dateTime, e);
        }
        return null;
    }

    public static String formatDatetime(LocalDateTime dateTime) {
        return dateTime.format(DATE_TIME_FORMATTER);
    }
}
