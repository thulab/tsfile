package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class NodeConstructorForSingleFileImpl extends NodeConstructor {
    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;

    public NodeConstructorForSingleFileImpl(MetadataQuerier metadataQuerier, SeriesChunkLoader seriesChunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.seriesChunkLoader = seriesChunkLoader;
    }

    @Override
    public SeriesReader generateSeriesReader(SeriesFilter<?> seriesFilter) throws IOException {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getSeriesChunkMetaDataList(
                seriesFilter.getSeriesPath());
        return new SeriesReaderFromSingleFileWithFilterImpl(seriesChunkLoader, chunkMetaDataList, seriesFilter.getFilter());
    }
}
