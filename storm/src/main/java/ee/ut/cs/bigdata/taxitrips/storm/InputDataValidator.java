package ee.ut.cs.bigdata.taxitrips.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class InputDataValidator {

    private static final Logger LOG = LoggerFactory.getLogger(InputDataValidator.class);

    public static boolean validateField(String fieldValue) {
        return fieldValue != null && !fieldValue.isEmpty() && !fieldValue.equals("null");
    }

    public static BigDecimal parseBigDecimal(String value) {
        if (validateField(value)) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                LOG.info("Failed to format bigdecimal " + value, e);
            }
        }
        return null;
    }
}
