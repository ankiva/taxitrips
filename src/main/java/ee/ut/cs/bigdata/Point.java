package ee.ut.cs.bigdata;

public class Point {
    private final int column;
    private final int row;

    private Point(int column, int row) {
        this.column = column;
        this.row = row;
    }

    public int getColumn(){
        return this.column;
    }

    public int getRow(){
        return this.row;
    }

    public static Point of(int column, int row) {
        return new Point(column, row);
    }
}
