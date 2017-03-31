package com.corp.delta.tsfile.filter.definition.operators;

import com.corp.delta.tsfile.filter.definition.filterseries.FilterSeries;
import com.corp.delta.tsfile.filter.visitorImpl.FilterVisitor;

/**
 * Greater than | Equals
 * 
 * @author CGF
 *
 * @param <T>
 */
public class GtEq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

	private static final long serialVersionUID = -2088181659871608986L;
	public boolean ifEq = false;

	public GtEq(FilterSeries<T> filterSeries, T value, Boolean ifEq) {
		super(filterSeries, value);
		this.ifEq = ifEq;
	}

	@Override
	public <R> R accept(FilterVisitor<R> vistor) {
		return vistor.visit(this);
	}

	public Boolean getIfEq() {
		return this.ifEq;
	}

	@Override
	public String toString() {
		if (ifEq)
			return filterSeries + " >= " + value;
		else
			return filterSeries + " > " + value;
	}
}
