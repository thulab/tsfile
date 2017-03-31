package com.corp.delta.tsfile.filter.definition.operators;

import com.corp.delta.tsfile.filter.definition.filterseries.FilterSeries;
import com.corp.delta.tsfile.filter.visitorImpl.FilterVisitor;

/**
 * Less than | Equals
 * 
 * @author CGF
 *
 * @param <T>
 */
public class LtEq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

	private static final long serialVersionUID = -6472106605198074799L;

	public Boolean ifEq = false; // To judge whether equals(true if LtEq
									// operator means less than and equals),
									// false by default

	public LtEq(FilterSeries<T> filterSeries, T value, Boolean ifEq) {
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
			return filterSeries + " <= " + value;
		else
			return filterSeries + " < " + value;
	}
}
