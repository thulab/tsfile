package cn.edu.tsinghua.tsfile.timeseries.filter.operator;


import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

/**
 * <code>NoRestriction</code> means that there is no filter.
 */
public class NoRestriction<T extends Comparable<T>> implements Filter<T> {
    private static final NoRestriction INSTANCE = new NoRestriction();

    public static final NoRestriction getInstance() {
        return INSTANCE;
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }
}
