package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithFilterImpl extends SeriesReaderFromSingleFile {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;

    public SeriesReaderFromSingleFileWithFilterImpl(SeriesChunkLoader seriesChunkLoader
            , List<SeriesChunkDescriptor> seriesChunkDescriptorList, Filter<?> filter) {
        super(seriesChunkLoader, seriesChunkDescriptorList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    protected void initSeriesChunkReader(SeriesChunkDescriptor seriesChunkDescriptor) throws IOException {
        MemSeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(seriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderWithFilterImpl(memSeriesChunk.getSeriesChunkBodyStream(),
                memSeriesChunk.getSeriesChunkDescriptor().getDataType(),
                memSeriesChunk.getSeriesChunkDescriptor().getCompressionTypeName(),
                filter);
    }

    @Override
    protected boolean seriesChunkSatisfied(SeriesChunkDescriptor seriesChunkDescriptor) {
        DigestForFilter timeDigest = new DigestForFilter(seriesChunkDescriptor.getMinTimestamp(),
                seriesChunkDescriptor.getMaxTimestamp());
        DigestForFilter valueDigest = new DigestForFilter(
                seriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                seriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                seriesChunkDescriptor.getDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }
}
