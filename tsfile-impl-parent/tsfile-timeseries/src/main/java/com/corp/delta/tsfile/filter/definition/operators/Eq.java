package com.corp.delta.tsfile.filter.definition.operators;

import com.corp.delta.tsfile.filter.definition.filterseries.FilterSeries;
import com.corp.delta.tsfile.filter.visitorImpl.FilterVisitor;

/**
 * Equals
 * 
 * @author CGF
 *
 * @param <T>
 */
public class Eq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

	private static final long serialVersionUID = -6668083116644568248L;

	public Eq(FilterSeries<T> filterSeries, T value) {
		super(filterSeries, value);
	}

	@Override
	public <R> R accept(FilterVisitor<R> vistor) {
		return vistor.visit(this);
	}

	@Override
	public String toString() {
		return filterSeries + " = " + value;
	}
}
