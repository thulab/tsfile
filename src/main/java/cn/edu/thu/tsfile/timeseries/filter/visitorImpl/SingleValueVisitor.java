package cn.edu.thu.tsfile.timeseries.filter.visitorImpl;

import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.thu.tsfile.timeseries.filter.utils.DoubleInterval;
import cn.edu.thu.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.utils.FloatInterval;
import cn.edu.thu.tsfile.timeseries.filter.utils.IntInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.thu.tsfile.timeseries.filter.verifier.FilterVerifier;

/**
 * To judge whether a single value satisfy the filter.</br>
 * Implemented per visitor pattern.
 * 
 * @author CGF
 *
 * @param <V>
 */
public class SingleValueVisitor<V extends Comparable<V>> implements FilterVisitor<Boolean> { 

    private V value;
    private FilterVerifier verifier;
    private SingleSeriesFilterExpression ssfilter;
    private static final Logger LOG = LoggerFactory.getLogger(SingleValueVisitor.class);
    
    public SingleValueVisitor() {}
 
    public SingleValueVisitor(SingleSeriesFilterExpression filter) {
        verifier = FilterVerifier.get(filter);
        this.ssfilter = filter;
    }

    public Boolean satisfy(V value, SingleSeriesFilterExpression filter) {
        this.value = value;
        return filter.accept(this);
    }

    /**
     * optimization of filter, filter -> value interval
     * 
     * @param value
     * @return
     */
    public boolean verify(int value) {
    	IntInterval val = (IntInterval) verifier.getInterval(ssfilter);
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false; 
    }

    public boolean verify(long value) {
    	LongInterval val = (LongInterval) verifier.getInterval(ssfilter);
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false; 
    }

    public boolean verify(float value) {
        FloatInterval val = (FloatInterval) verifier.getInterval(ssfilter);
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    public boolean verify(double value) {
        DoubleInterval val = (DoubleInterval) verifier.getInterval(ssfilter);
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    /**
     * This method exits a problem, the data type of value must accord with filter.
     *
     * @param value
     * @param filter
     * @return
     */
    public Boolean satisfyObject(Object value, SingleSeriesFilterExpression filter) {
        // The value type and filter type may not be consistent
        return this.satisfy((V) value, filter);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        if (eq.getValue().equals(value))
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        if (!notEq.getValue().equals(value))
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (ltEq.getIfEq() && ltEq.getValue().compareTo((T) value) >= 0)
            return true;
        if (!ltEq.getIfEq() && ltEq.getValue().compareTo((T) value) > 0)
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (gtEq.getIfEq() && gtEq.getValue().compareTo((T) value) <= 0)
            return true;
        if (!gtEq.getIfEq() && gtEq.getValue().compareTo((T) value) < 0)
            return true; 
        return false;
    }

    @Override
    public Boolean visit(Not not) {
        if (satisfy(value, not.getFilterExpression()))
            return false;
        return true;
    }

    @Override
    public Boolean visit(And and) {
        return satisfy(value, and.getLeft()) && satisfy(value, and.getRight());
    }

    @Override
    public Boolean visit(Or or) {
        return satisfy(value, or.getLeft()) || satisfy(value, or.getRight());
    }

}
