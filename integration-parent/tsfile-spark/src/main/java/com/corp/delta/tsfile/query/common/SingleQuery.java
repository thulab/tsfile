package com.corp.delta.tsfile.query.common;


/**
 * This class is constructed with a single getIndex plan. Single getIndex means it could be processed by
 * reading API by one pass directly.<br>
 *
 */
public class SingleQuery {

    private FilterOperator deltaObjectFilterOperator;
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public SingleQuery(FilterOperator deltaObjectFilterOperator,
                       FilterOperator timeFilter, FilterOperator valueFilter) {
        super();
        this.deltaObjectFilterOperator = deltaObjectFilterOperator;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public FilterOperator getDeltaObjectFilterOperator() {

        return deltaObjectFilterOperator;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    @Override
    public String toString() {
        return "SingleQuery: \n" + deltaObjectFilterOperator + "\n" + timeFilterOperator + "\n" + valueFilterOperator;
    }


}
