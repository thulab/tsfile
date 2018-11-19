package cn.edu.tsinghua.tsfile.timeseries.filter.factory;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.Or;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public class FilterFactory {
    public static <T extends Comparable<T>> Filter<T> and(Filter<T> left, Filter<T> right){
        return new And(left, right);
    }

    public static <T extends Comparable<T>> Filter<T> or(Filter<T> left, Filter<T> right){
        return new Or(left, right);
    }

    public static <T extends Comparable<T>> Filter<T> not(Filter<T> filter) {
        return new Not<>(filter);
    }

}
