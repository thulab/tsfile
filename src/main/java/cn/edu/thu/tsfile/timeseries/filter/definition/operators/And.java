package cn.edu.thu.tsfile.timeseries.filter.definition.operators;

import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Both the left and right operators of And must satisfy the condition.
 * 
 * @author CGF
 *
 */
public class And extends SingleBinaryExpression {

	private static final long serialVersionUID = 6705254093824897938L;

	public And(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
		super(left, right);
	}

	@Override
	public <T> T accept(FilterVisitor<T> vistor) {
		return vistor.visit(this);
	}

	@Override
	public String toString() {
		return "AND: ( " + left + "," + right + " )";
	}

	@Override
	public FilterSeries<?> getFilterSeries() {
		return this.left.getFilterSeries();
	}
}
