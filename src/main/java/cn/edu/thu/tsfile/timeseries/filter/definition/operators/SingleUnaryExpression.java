package cn.edu.thu.tsfile.timeseries.filter.definition.operators;

import java.io.Serializable;

import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;
import cn.edu.thu.tsfile.common.exception.FilterInvokeException;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Definition for unary filter operations of single series.
 * 
 * @author CGF
 *
 * @param <T>
 */
public class SingleUnaryExpression<T extends Comparable<T>> extends SingleSeriesFilterExpression
		implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(SingleUnaryExpression.class);
	private static final long serialVersionUID = 1431606024929453556L;
	protected final FilterSeries<T> filterSeries;
	protected final T value;

	protected SingleUnaryExpression(FilterSeries<T> filterSeries, T value) {
		this.filterSeries = filterSeries;
		this.value = value;
	}

	public FilterSeries<T> getFilterSeries() {
		return filterSeries;
	}

	public T getValue() {
		return value;
	}

	@Override
	public String toString() {
		return filterSeries + " - " + value;
	}

	@SuppressWarnings("hiding")
	@Override
	public <T> T accept(FilterVisitor<T> vistor) {
		// Never be invoked
		// This method is invoked by specific UnarySeriesFilter which is
		// subclass of UnarySeriesFilter,
		// such as LtEq, Eq..
		LOG.error("UnarySeriesFilter's accept method can never be invoked.");
		throw new FilterInvokeException("UnarySeriesFilter's accept method can never be invoked.");
	}
}
