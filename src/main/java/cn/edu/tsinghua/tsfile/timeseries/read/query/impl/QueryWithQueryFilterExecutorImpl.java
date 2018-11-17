package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * @author Jinrui Zhang
 */
public class QueryWithQueryFilterExecutorImpl implements QueryExecutor {

    private SeriesChunkLoader seriesChunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithQueryFilterExecutorImpl(SeriesChunkLoader seriesChunkLoader, MetadataQuerier metadataQuerier) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        TimestampGenerator timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryExpression.getQueryFilter(),
                seriesChunkLoader, metadataQuerier);
        LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new QueryDataSetForQueryWithQueryFilterImpl(timestampGenerator, readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderFromSingleFileByTimestampImpl> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException {
        for (Path path : selectedSeries) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getSeriesChunkMetaDataList(path);
            SeriesReaderFromSingleFileByTimestampImpl seriesReader = new SeriesReaderFromSingleFileByTimestampImpl(
                    seriesChunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}