package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderWithFilterImpl extends SeriesChunkReader {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;
    private TimeValuePairFilterVisitor<Boolean> timeValuePairFilterVisitor;

    public SeriesChunkReaderWithFilterImpl(ByteBuffer seriesChunkByteBuffer, Filter<?> filter) {
        super(seriesChunkByteBuffer);
        this.filter = filter;
        this.timeValuePairFilterVisitor = new TimeValuePairFilterVisitorImpl();
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        if (pageHeader.getMax_timestamp() < getMaxTombstoneTime())
            return false;
        DigestForFilter timeDigest = new DigestForFilter(pageHeader.getMin_timestamp(),
                pageHeader.getMax_timestamp());
        //TODO: Using ByteBuffer as min/max is best
        DigestForFilter valueDigest = new DigestForFilter(
                pageHeader.getStatistics().getMinBytebuffer(),
                pageHeader.getStatistics().getMaxBytebuffer(),
                chunkHeader.getDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        if (timeValuePair.getTimestamp() < getMaxTombstoneTime())
            return false;
        return timeValuePairFilterVisitor.satisfy(timeValuePair, filter);
    }
}
