package com.corp.delta.tsfile.filter.verifier;

import com.corp.delta.tsfile.filter.definition.SingleSeriesFilterExpression;
import com.corp.delta.tsfile.filter.utils.Interval;

/**
 * optimizing of filter, transfer SingleSensorFilter to interval comparison
 * see {@link Interval}
 * 
 * @author CGF
 * 
 *
 */
public abstract class FilterVerifier {

    public static FilterVerifier get(SingleSeriesFilterExpression filter) {
        switch (filter.getFilterSeries().getSeriesDataType()) {
            case INT32:
                return new IntFilterVerifier();
            case INT64:
                return new LongFilterVerifier();
            case FLOAT:
                return new FloatFilterVerifier();
            case DOUBLE:
                return new DoubleFilterVerifier();
            default:
                return null;
        }
    }

    public abstract Interval getInterval(SingleSeriesFilterExpression filter);
}
