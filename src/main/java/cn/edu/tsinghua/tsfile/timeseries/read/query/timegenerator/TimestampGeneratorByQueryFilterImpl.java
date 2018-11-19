package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.Node;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class TimestampGeneratorByQueryFilterImpl implements TimestampGenerator {

    private QueryFilter queryFilter;
    private Node operatorNode;

    public TimestampGeneratorByQueryFilterImpl(QueryFilter queryFilter, SeriesChunkLoader seriesChunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.queryFilter = queryFilter;
        initNode(seriesChunkLoader, metadataQuerier);
    }

    private void initNode(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) throws IOException {
        NodeConstructorForSingleFileImpl nodeConstructorForSingleFile = new NodeConstructorForSingleFileImpl(metadataQuerier, seriesChunkLoader);
        this.operatorNode = nodeConstructorForSingleFile.construct(queryFilter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }
}
