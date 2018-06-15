package cn.edu.tsinghua.ts2file.timesegment.filter;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NoFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 *
 * @author qmm
 */
public class SegmentTimeVisitor implements FilterVisitor<Boolean> {

    private Long startTime, endTime;

    public boolean satisfy(SingleSeriesFilterExpression timeFilter, Long s, Long e) {
        if (timeFilter == null) {
            return true;
        }

        this.startTime = s;
        this.endTime = e;
        return timeFilter.accept(this);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        return eq.getValue().equals(startTime) && eq.getValue().equals(endTime);

    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        return (Long) notEq.getValue() < startTime && (Long) notEq.getValue() > endTime;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (ltEq.getIfEq()) {
            return (Long) ltEq.getValue() >= endTime;
        } else {
            return (Long) ltEq.getValue() > endTime;
        }
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (gtEq.getIfEq()) {
            return (Long) gtEq.getValue() <= startTime;
        } else {
            return (Long) gtEq.getValue() < startTime;
        }
    }

    @Override
    public Boolean visit(Not not) {
        return !satisfy(not.getFilterExpression(), startTime, endTime);
    }

    @Override
    public Boolean visit(And and) {
        return satisfy(and.getLeft(), startTime, endTime) && satisfy(and.getRight(), startTime, endTime);
    }

    @Override
    public Boolean visit(Or or) {
        return satisfy(or.getLeft(), startTime, endTime) || satisfy(or.getRight(), startTime, endTime);
    }

    @Override
    public Boolean visit(NoFilter noFilter) {
        return true;
    }

}
