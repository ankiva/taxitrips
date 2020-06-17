package ee.ut.cs.bigdata.taxitrips;

public class Cell {
    private final int column;
    private final int row;

    private Cell(int column, int row) {
        this.column = column;
        this.row = row;
    }

    public int getColumn() {
        return this.column;
    }

    public int getRow() {
        return this.row;
    }

    public static Cell of(int column, int row) {
        return new Cell(column, row);
    }

    @Override
    public String toString() {
        return column + "." + row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cell cell = (Cell) o;

        if (column != cell.column) return false;
        return row == cell.row;
    }

    @Override
    public int hashCode() {
        int result = column;
        result = 31 * result + row;
        return result;
    }
}
