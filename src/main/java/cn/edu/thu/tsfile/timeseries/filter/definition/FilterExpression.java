package cn.edu.thu.tsfile.timeseries.filter.definition;

import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * FilterExpression is a top level filter abstraction.</br>
 * FilterExpression has two types of subclass : {@link SingleSeriesFilterExpression} and
 * {@link CrossSeriesFilterExpression}.</br>
 * FilterExpression is a role of interviewee in visitor pattern.
 * 
 * @author CGF
 *
 */
public interface FilterExpression {
    /**
     * All subclass of
     * {@link SingleBinaryExpression.SingleFilterBinaryExpression} and
     * {@link SingleBinaryExpression.SingleFilterBinaryExpression} must
     * accept a FilterVistor, per the visitor pattern.
     */
    public abstract <T> T accept(FilterVisitor<T> vistor);

    /**
     * FilterSeries get method.
     *
     * @return FilterSeries
     */
    public abstract FilterSeries<?> getFilterSeries();
}
