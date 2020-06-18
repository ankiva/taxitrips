package ee.ut.cs.bigdata.taxitrips.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

public class TaxiDatetimeFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(TaxiDatetimeFormatter.class);

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    public static Instant parseDatetime(String dateTime) {
        try {
            return DATE_TIME_FORMATTER.parse(dateTime, Instant::from);
        } catch (DateTimeParseException e) {
            LOG.info("Failed to parse date " + dateTime, e);
        }
        return null;
    }

    public static String formatDatetime(Instant dateTime) {
        return DATE_TIME_FORMATTER.format(dateTime);
    }
}
