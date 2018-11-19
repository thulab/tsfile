package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryWithGlobalTimeFilterExecutorImpl implements QueryExecutor {

    private SeriesChunkLoader seriesChunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithGlobalTimeFilterExecutorImpl(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        Filter<Long> timeFilter = ((GlobalTimeFilter) queryExpression.getQueryFilter()).getFilter();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), timeFilter);
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries, Filter<Long> timeFilter) throws IOException {
        for (Path path : selectedSeries) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getSeriesChunkMetaDataList(path);
            SeriesReader seriesReader = new SeriesReaderFromSingleFileWithFilterImpl(seriesChunkLoader, chunkMetaDataList, timeFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
