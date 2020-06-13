package ee.ut.cs.bigdata.taxitrips;

public class Point {
    private final int column;
    private final int row;

    private Point(int column, int row) {
        this.column = column;
        this.row = row;
    }

    public int getColumn() {
        return this.column;
    }

    public int getRow() {
        return this.row;
    }

    public static Point of(int column, int row) {
        return new Point(column, row);
    }

    @Override
    public String toString() {
        return "P(" + column +
                "." + row +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point point = (Point) o;

        if (column != point.column) return false;
        return row == point.row;
    }

    @Override
    public int hashCode() {
        int result = column;
        result = 31 * result + row;
        return result;
    }
}
