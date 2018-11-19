package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public abstract class NodeConstructor {

    public Node construct(QueryFilter queryFilter) throws IOException {
        if (queryFilter.getType() == QueryFilterType.SERIES) {
            return new LeafNode(generateSeriesReader((SeriesFilter) queryFilter));
        } else if (queryFilter.getType() == QueryFilterType.OR) {
            Node leftChild = construct(((BinaryQueryFilter) queryFilter).getLeft());
            Node rightChild = construct(((BinaryQueryFilter) queryFilter).getRight());
            return new OrNode(leftChild, rightChild);
        } else if (queryFilter.getType() == QueryFilterType.AND) {
            Node leftChild = construct(((BinaryQueryFilter) queryFilter).getLeft());
            Node rightChild = construct(((BinaryQueryFilter) queryFilter).getRight());
            return new AndNode(leftChild, rightChild);
        }
        throw new UnSupportedDataTypeException("Unsupported QueryFilterType when construct OperatorNode: " + queryFilter.getType());
    }

    public abstract SeriesReader generateSeriesReader(SeriesFilter<?> seriesFilter) throws IOException;

}
