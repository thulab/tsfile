package cn.edu.thu.tsfile.timeseries.filter.definition.operators;

import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Not Equals
 * 
 * @author CGF
 *
 * @param <T>
 */
public class NotEq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

	private static final long serialVersionUID = 2574090797476500965L;

	public NotEq(FilterSeries<T> filterSeries, T value) {
		super(filterSeries, value);
	}

	@Override
	public <R> R accept(FilterVisitor<R> vistor) {
		return vistor.visit(this);
	}

	@Override
	public String toString() {
		return filterSeries + " != " + value;
	}
}
