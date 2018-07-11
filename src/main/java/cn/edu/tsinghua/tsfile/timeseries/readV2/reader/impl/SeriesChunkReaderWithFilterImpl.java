package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.TimeValuePairFilterVisitorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderWithFilterImpl extends SeriesChunkReader {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;
    private TimeValuePairFilterVisitor<Boolean> timeValuePairFilterVisitor;

    public SeriesChunkReaderWithFilterImpl(InputStream seriesChunkInputStream, TSDataType dataType,
                                           CompressionType compressionType, TSEncoding dataEncoding, Filter<?> filter) {
        super(seriesChunkInputStream, dataType, compressionType, dataEncoding);
        this.filter = filter;
        this.timeValuePairFilterVisitor = new TimeValuePairFilterVisitorImpl();
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        DigestForFilter timeDigest = new DigestForFilter(pageHeader.getMin_timestamp(),
                pageHeader.getMax_timestamp());
        //TODO: Using ByteBuffer as min/max is better
        DigestForFilter valueDigest = new DigestForFilter(
                pageHeader.getStatistics().getMinBytebuffer(),
                pageHeader.getStatistics().getMaxBytebuffer(),
                dataType);
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return timeValuePairFilterVisitor.satisfy(timeValuePair, filter);
    }
}
