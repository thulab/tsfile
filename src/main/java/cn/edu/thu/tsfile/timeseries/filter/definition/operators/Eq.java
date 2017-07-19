package cn.edu.thu.tsfile.timeseries.filter.definition.operators;

import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Equals
 *
 * @param <T>
 * @author CGF
 */
public class Eq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

    private static final long serialVersionUID = -6668083116644568248L;

    public Eq(FilterSeries<T> filterSeries, T value) {
        super(filterSeries, value);
    }

    @Override
    public <R> R accept(FilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return filterSeries + " = " + value;
    }
}
