package ee.ut.cs.bigdata.taxitrips.storm.util;

public class ImmutablePair<L, R> {
    private final L left;
    private final R right;

    public ImmutablePair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return this.left;
    }

    public R getRight() {
        return this.right;
    }

    @Override
    public String toString() {
        return "(" + this.left + ", " + this.right + ")";
    }
}
