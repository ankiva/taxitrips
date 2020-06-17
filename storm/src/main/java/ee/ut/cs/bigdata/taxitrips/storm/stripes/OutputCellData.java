package ee.ut.cs.bigdata.taxitrips.storm.stripes;

import java.math.BigDecimal;

public class OutputCellData {
    private final String cellId;

    private String profit;

    private String emptyTaxies;

    private BigDecimal profitability;

    public OutputCellData(String cellId) {
        this.cellId = cellId;
    }

    public String getCellId() {
        return this.cellId;
    }

    public String getProfit() {
        return profit;
    }

    public void setProfit(String profit) {
        this.profit = profit;
    }

    public String getEmptyTaxies() {
        return emptyTaxies;
    }

    public void setEmptyTaxies(String emptyTaxies) {
        this.emptyTaxies = emptyTaxies;
    }

    public BigDecimal getProfitability() {
        return profitability;
    }

    public void setProfitability(BigDecimal profitability) {
        this.profitability = profitability;
    }
}
