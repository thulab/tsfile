package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Not necessary. Use InvertExpressionVisitor
 *
 * @author CGF
 */
public class Not<T extends Comparable<T>> implements Filter<T>, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public Not(Filter that) {
        this.that = that;
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }

    public Filter getFilterExpression() {
        return this.that;
    }

    @Override
    public String toString() {
        return "Not: " + that;
    }

}
