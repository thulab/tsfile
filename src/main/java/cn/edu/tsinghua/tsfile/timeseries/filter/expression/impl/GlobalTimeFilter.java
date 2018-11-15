package cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.UnaryQueryFilter;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public class GlobalTimeFilter implements UnaryQueryFilter {
    private Filter<Long> filter;

    public GlobalTimeFilter(Filter<Long> filter) {
        this.filter = filter;
    }

    public Filter<Long> getFilter() {
        return filter;
    }

    public void setFilter(Filter<Long> filter) {
        this.filter = filter;
    }

    @Override
    public QueryFilterType getType() {
        return QueryFilterType.GLOBAL_TIME;
    }

    public String toString() {
        return "[" + this.filter.toString() + "]";
    }
}
