package cn.edu.tsinghua.tsfile.timeseries.filter.expression;

/**
 * @author Jinrui Zhang
 */
public interface BinaryQueryFilter extends QueryFilter{
    QueryFilter getLeft();

    QueryFilter getRight();


}
