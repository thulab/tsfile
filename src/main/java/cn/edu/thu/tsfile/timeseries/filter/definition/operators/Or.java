package cn.edu.thu.tsfile.timeseries.filter.definition.operators;

import java.io.Serializable;

import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Either of the left and right operators of And must satisfy the condition.
 * 
 * @author CGF
 *
 */
public class Or extends SingleBinaryExpression implements Serializable {

	private static final long serialVersionUID = -968055896528472694L;

	public Or(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
		super(left, right);
	}

	@Override
	public <T> T accept(FilterVisitor<T> vistor) {
		return vistor.visit(this);
	}

	@Override
	public String toString() {
		return "OR: ( " + left + "," + right + " )";
	}

	@Override
	public FilterSeries<?> getFilterSeries() {
		return left.getFilterSeries();
	}

}
