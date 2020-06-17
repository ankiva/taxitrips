package ee.ut.cs.bigdata.taxitrips.storm.util;

import java.math.BigDecimal;

public class CompleteOutputRecord {
    public final String cellId;
    public final Integer emptyTaxies;
    public final String medianProfit;
    public final BigDecimal profitability;

    public CompleteOutputRecord(String cellId, Integer emptyTaxies, String medianProfit, BigDecimal profitability){
        this.cellId = cellId;
        this.emptyTaxies = emptyTaxies;
        this.medianProfit = medianProfit;
        this.profitability = profitability;
    }

    @Override
    public String toString() {
        return "CompleteOutputRecord{" +
                "cellId='" + cellId + '\'' +
                ", emptyTaxies=" + emptyTaxies +
                ", medianProfit='" + medianProfit + '\'' +
                ", profitability=" + profitability +
                '}';
    }
}
