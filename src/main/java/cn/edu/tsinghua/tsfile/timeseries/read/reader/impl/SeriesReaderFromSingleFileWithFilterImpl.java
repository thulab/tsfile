package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithFilterImpl extends SeriesReaderFromSingleFile {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;

    public SeriesReaderFromSingleFileWithFilterImpl(SeriesChunkLoader seriesChunkLoader
            , List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList, Filter<?> filter) {
        super(seriesChunkLoader, timeSeriesChunkMetaDataList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(TsFileSequenceReader tsFileReader, SeriesChunkLoader seriesChunkLoader,
                                                    List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList, Filter<?> filter) {
        super(tsFileReader, seriesChunkLoader, timeSeriesChunkMetaDataList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(TsFileSequenceReader tsFileReader
            , Path path, Filter<?> filter) throws IOException {
        super(tsFileReader, path);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    protected void initSeriesChunkReader(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(timeSeriesChunkMetaData);
        this.seriesChunkReader = new SeriesChunkReaderWithFilterImpl(memSeriesChunk.getSeriesChunkBodyStream(),
                filter);
        this.seriesChunkReader.setMaxTombstoneTime(timeSeriesChunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        DigestForFilter timeDigest = new DigestForFilter(timeSeriesChunkMetaData.getStartTime(),
                timeSeriesChunkMetaData.getEndTime());
        //TODO: Using ByteBuffer as min/max is best
        DigestForFilter valueDigest = new DigestForFilter(
                timeSeriesChunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                timeSeriesChunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                timeSeriesChunkMetaData.getTsDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }
}
