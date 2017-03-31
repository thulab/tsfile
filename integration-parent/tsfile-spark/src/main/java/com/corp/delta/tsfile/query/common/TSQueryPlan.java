package com.corp.delta.tsfile.query.common;

import java.util.ArrayList;
import java.util.List;

/**
 * One tsfile logical query plan that can be performed at one time
 *
 */
public class TSQueryPlan {
    private List<String> paths = new ArrayList<>();
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public TSQueryPlan(List<String> paths, FilterOperator timeFilter, FilterOperator valueFilter) {
        this.paths = paths;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public List<String> getPaths() {
        return paths;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    public String toString(){
        return "paths:\n" + paths.toString() +
                "\ntimefilter:\n" + timeFilterOperator.toString() +
                "\nvaluefilter\n" + valueFilterOperator.toString();
    }
}
